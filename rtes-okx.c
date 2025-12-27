#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <inttypes.h>
#include <stddef.h>
#include <limits.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <libwebsockets.h>
#include <jansson.h>

#define UNUSED(x) (void)(x)

enum instrument_metrics {
    UNDEFINED = -1,         // Used as a marker in instances of non-existent Pearson correlation coefficient entries
    TRADE,                  // File index for the raw trade data
    MOVAVG,                 // File index for the moving average and size data
    PEARSON,                // File index for the Pearson correlation coefficient data
    METRICS,                // Number of metrics we'll be recording (raw trade data, moving average values, Pearson correlation coefficient values), each corresponding to a file
    DATAPOINTS = 8          // Number of datapoints kept for Pearson correlation coefficient calculations
};

enum program_parameters {
    THREADS = 3,            // Number of threads excluding the main thread
    SUBSCRIPTIONS = 8,      // Number of instruments we'll be tracking
    SUBSIZE = 10,           // Maximum string size of the instrument IDs we'll be subscribing to
    INTERVAL = 60,          // Time interval for the "perInterval" thread, in seconds
    INTERVALS = 15,         // Number of intervals over which certain metrics are calculated
    MAX_LINE = 128,         // Buffer size for reading moving-average-file lines
    QUEUESIZE = 16384       // Size of the FIFO queue used for the incoming WebSocket data
};

enum status_codes {
    FALSE,                  // For terminationFlags
    TRUE,                   // For terminationFlags
    CALLBACK_SUCCESS = 0,   // callbackOKX() returns this if all went ok
    JSON_LOADS_ERROR,       // callbackOKX() returns this if json_loads() fails
    OKX_ERROR               // callbackOKX() returns this if the data sent from OKX reads "error"
};

enum db_status {            // databaseInit() returns this if:
    DATABASE_SUCCESS,       // all went ok
    MKDIR_ERROR,            // mkdir() failed
    SNPRINTF_ERROR,         // snprintf() failed
    FOPEN_ERROR,            // fopen() failed
    FPRINTF_ERROR           // fprintf() failed
};

// Struct for the incoming trade data from OKX
typedef struct {
    int subIndex, count;
    double price, size;
    unsigned long long timestamp, timeReceived;
} trade;

// Will be used for the per-interval calculation & storage of the maximum Pearson correlation coefficient
typedef struct {
    double coeff;
    unsigned long long corrTime;
} pearson_corr;

// Struct which will contain aggregate data of the current interval, as well as the files where the data will eventually be saved
// Used by the "consumer" and "perInterval" thread
typedef struct {
    FILE* files[METRICS][SUBSCRIPTIONS];
    double sumPrices[SUBSCRIPTIONS], totalSize[SUBSCRIPTIONS];
    int numTrades[SUBSCRIPTIONS];
    pthread_mutex_t mut;
} database;

// FIFO queue for the trade data
typedef struct {
    trade buf[QUEUESIZE];
    long head, tail;
    int full, empty;
    pthread_mutex_t mut;
    pthread_cond_t notFull, notEmpty;
} queue;

// Contains all the parameters necessary to (re-)establish WebSocket connections
typedef struct {
    lws_sorted_usec_list_t sul;
    struct lws *wsi;
    struct lws_client_connect_info info;
    lws_retry_bo_t retry;
    uint16_t retry_count;
} client_connect_data;

// All the data to be passed to the "producer" thread
typedef struct {
    client_connect_data clientConnectData;
    queue *fifo;
    int terminationFlag, subCount;
    const char subscriptions[SUBSCRIPTIONS][SUBSIZE];
    char subList[SUBSCRIPTIONS][SUBSIZE];
} producer_data;

// All the data to be passed to the "consumer" thread
typedef struct {
    database DB;
    queue fifo;
    int terminationFlag;
} consumer_data;

// All the data to be passed to the "perInterval" thread
typedef struct {
    database *DB;
    int terminationFlag;
    const char (*subs)[SUBSCRIPTIONS][SUBSIZE];
    pthread_cond_t termination;
} per_interval_data;

// Struct that serves as an information package for creating and terminating threads
typedef struct {
    pthread_t id;
    pthread_mutex_t *mut;
    pthread_cond_t *cond;
    int *flag;
    const char* const name;
    void *(*func)(void*);
    void *args;
} thread_info;

static int strtoi(const char *s);
static unsigned long long unixTimeInMs();

static double pearsonCorrCoeff(int n, double X[n], double Y[n]);
static int checkPearsonMax(pearson_corr pearson, pearson_corr *maxPearson, int currentBestIndicator, int instrumentBeingChecked);

static enum db_status databaseInit(database *db, const char subsArray[SUBSCRIPTIONS][SUBSIZE]);
static void databaseDelete(database *db);
static void connectClient(lws_sorted_usec_list_t *sul);

static void *producer(void *lwsContextStruct);
static void *consumer(void *consumerDataStruct);
static void *perInterval(void *perIntervalDataStruct);

static void queueInit(queue *q);
static void queueDelete(queue *q);
static void queueAdd(queue *q, trade in);
static void queueDel(queue *q, trade *out);

// Callback function that processes WebSocket events
static int callbackOKX(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len) {
    switch (reason) {
        // When the connection is first established, fill a list of instruments to subscribe to and schedule a write-to-server event
        case LWS_CALLBACK_CLIENT_ESTABLISHED: {
            printf("\nWebSocket connection established\n");
            producer_data *producerData = lws_context_user(lws_get_context(wsi)); // retrieve producer data from the LWS context; attached during initialization in main
            producerData->subCount = 0;
            memcpy(producerData->subList, producerData->subscriptions, sizeof(producerData->subscriptions));
            lws_callback_on_writable(wsi);
            return CALLBACK_SUCCESS;
        }

        // If the server is writeable and not every required instrument has been subscribed to, subscribe to one of them
        case LWS_CALLBACK_CLIENT_WRITEABLE: {
            producer_data *producerData = lws_context_user(lws_get_context(wsi));
            if (producerData->subCount < SUBSCRIPTIONS) {
                char subSpec[sizeof("{\"op\":\"subscribe\",\"args\": [{\"channel\": \"trades\",\"instId\": \"\"}]}") +
                             strlen(producerData->subList[producerData->subCount])];

                while (snprintf(subSpec, sizeof(subSpec), "{\"op\":\"subscribe\",\"args\": [{\"channel\": \"trades\",\"instId\": \"%s\"}]}",
                         producerData->subList[producerData->subCount]) < 0);

                unsigned char buf[LWS_SEND_BUFFER_PRE_PADDING + sizeof(subSpec) + LWS_SEND_BUFFER_POST_PADDING];
                memset(buf, 0, sizeof(buf));
                memcpy(&buf[LWS_SEND_BUFFER_PRE_PADDING], subSpec, sizeof(subSpec));

                //printf("Sending: %s\n", subSpec);
                lws_write(wsi, &buf[LWS_SEND_BUFFER_PRE_PADDING], sizeof(subSpec), LWS_WRITE_TEXT);
            }
            return CALLBACK_SUCCESS;
        }

        // In case of incoming message from the server
        case LWS_CALLBACK_CLIENT_RECEIVE: {
            if (strncmp((const char *)in, "ping", sizeof("ping")) == 0) {
                printf("Received:  Ping\n");
                return CALLBACK_SUCCESS;
            }

            // Messages come in JSON format
            json_t *root;
            json_error_t error;

            root = json_loads((const char *)in, 0, &error);

            if (!root) {
                fprintf(stderr, "Error: on line %d: %s\n", error.line, error.text);
                json_decref(root);
                return JSON_LOADS_ERROR;
            }

            json_t *type = json_object_get(root, "event");

            /* Looking at the OKX WebSocket API documentation, trade data messages have no "type" attribute;
               this is used to distinguish between the different kinds of messages */
            if (type != NULL) {
                const char *type_value = json_string_value(type);

                if (strncmp(type_value, "error", sizeof("error")) == 0) {
                    printf("Received:  Error\n");
                    json_decref(root);
                    return OKX_ERROR;
                }
                producer_data *producerData = lws_context_user(lws_get_context(wsi));

                // A "subscribe" message means an instrument has been successfully subscribed to
                if (strncmp(type_value, "subscribe", sizeof("subscribe")) == 0) {
                    json_t *sub = json_object_get(root, "arg");

                    printf("Received:   Event: %s\tSymbol: %s\tChannel: %s\t\tConnectionID: %s\n",
                            type_value,
                            (const char *)json_string_value(json_object_get(sub, "instId")),
                            (const char *)json_string_value(json_object_get(sub, "channel")),
                            (const char *)json_string_value(json_object_get(root, "connId")));

                    // Increment the subscription counter. If the counter has not reached the defined number, schedule another write event
                    if (++producerData->subCount < SUBSCRIPTIONS) {
                        lws_callback_on_writable(wsi);
                    }
                }
                // An "unsubscribe" message means an instrument has (somehow) been unsubscribed from (never happened in practice)
                else if (strncmp(type_value, "unsubscribe", sizeof("unsubscribe")) == 0) {
                    json_t *sub = json_object_get(root, "arg");
                    const char *instId = (const char *)json_string_value(json_object_get(sub, "instId"));

                    printf("Received:   Event: %s\tSymbol: %s\tChannel: %s\t\tConnectionID: %s\n",
                            type_value,
                            instId,
                            (const char *)json_string_value(json_object_get(sub, "channel")),
                            (const char *)json_string_value(json_object_get(root, "connId")));

                    /* Decrement the subscription counter and copy the instrument-ID of the unsubscribed-from instrument to "subList",
                       which is the list containing the instruments to subscribe to */
                    producerData->subList[--producerData->subCount][0] = '\0';
                    strncat(producerData->subList[producerData->subCount], instId, SUBSIZE-1);
                    lws_callback_on_writable(wsi); // schedule a write event
                    UNUSED(user);
                }
            }

            else {
                json_t *data = json_object_get(root, "data");

                if (data != NULL) {
                    // Incoming non-NULL trade data handled according to the OKX WebSocket API documentation
                    json_t *metrics = json_array_get(data, 0);

                    json_t *instId = json_object_get(metrics, "instId");
                    const char *instId_value = (const char *)json_string_value(instId);

                    json_t *cnt_obj = json_object_get(metrics, "count"); // one message may aggregate multiple trades; this is how many were aggregated
                    json_t *px_obj = json_object_get(metrics, "px"); // trade price; if many trades were aggregated, this is the price of each one (it's the same for all of them, hence why they were grouped)
                    json_t *sz_obj = json_object_get(metrics, "sz"); // trade size; if many trades were aggregated, this is the TOTAL size of ALL of them combined
                    json_t *ts_obj = json_object_get(metrics, "ts"); // UNIX timestamp of when the trade was completed, in milliseconds

                    producer_data *producerData = lws_context_user(lws_get_context(wsi));

                    // Iterate through the (const) "subscriptions" array until the index that the trade's instrument-ID corresponds to is found
                    for (int i = 0; i < SUBSCRIPTIONS; ++i) {
                        if (strncmp(instId_value, producerData->subscriptions[i], SUBSIZE) == 0) {
                            trade currentTrade;

                            currentTrade.subIndex = i; // mark which instrument this trade belongs to using the "subscriptions" array index that was found
                            currentTrade.count = strtoi((const char *)json_string_value(cnt_obj));
                            currentTrade.price = strtod((const char *)json_string_value(px_obj), NULL);
                            currentTrade.size = strtod((const char *)json_string_value(sz_obj), NULL);
                            currentTrade.timestamp = strtoull((const char *)json_string_value(ts_obj), NULL, 10);
                            currentTrade.timeReceived = unixTimeInMs();

                            pthread_mutex_lock(&producerData->fifo->mut); // protect against conflict with consumer thread
                            while (producerData->fifo->full && !producerData->terminationFlag) { // protects against spurious wake-up
                                printf("producer: queue FULL.\n");	// never happened in practice (with the defined QUEUESIZE)
                                pthread_cond_wait(&producerData->fifo->notFull, &producerData->fifo->mut);
                            }
                            // Non-spurious wake-up + queue is full = program has initiated shutdown
                            if (producerData->fifo->full) {
                                pthread_mutex_unlock(&producerData->fifo->mut);
                                json_decref(root);
                                return CALLBACK_SUCCESS;
                            }
                            queueAdd(producerData->fifo, currentTrade);
                            pthread_cond_signal(&producerData->fifo->notEmpty);
                            pthread_mutex_unlock(&producerData->fifo->mut);
                            json_decref(root);
                            return CALLBACK_SUCCESS;
                        }
                    }
                }
                else {
                    printf("Received null data.\n"); // never happened in practice
                }
            }
            json_decref(root);
            return CALLBACK_SUCCESS;
        }

        // If the connection is randomly closed, attempt reconnection indefinitely until an attempt succeeds
        case LWS_CALLBACK_CLIENT_CLOSED: {
            printf("WebSocket connection closed\n");
            producer_data *producerData = lws_context_user(lws_get_context(wsi));
            if (!producerData->terminationFlag) {
                lws_retry_sul_schedule_retry_wsi(wsi, &producerData->clientConnectData.sul, connectClient,
                                                 &producerData->clientConnectData.retry_count); // see the connectClient function for more details
            }
            return CALLBACK_SUCCESS;
        }

        // If the connection is closed due to error, attempt reconnection indefinitely until an attempt succeeds
        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR: {
            if (in != NULL && len < INT_MAX) {
                fprintf(stderr, "WebSocket connection error: %.*s\n", (int)len, (const char*)in);
            }
            else {
                fprintf(stderr, "WebSocket connection error\n");
            }
            producer_data *producerData = lws_context_user(lws_get_context(wsi));
            if (!producerData->terminationFlag) {
                lws_retry_sul_schedule_retry_wsi(wsi, &producerData->clientConnectData.sul, connectClient,
                                                 &producerData->clientConnectData.retry_count); // see the connectClient function for more details
            }
            return CALLBACK_SUCCESS;
        }

        default: {
            return CALLBACK_SUCCESS;
        }
    }

    return CALLBACK_SUCCESS;
}

int main() {
    int exitCode = EXIT_SUCCESS;
/*  The program is designed to run until manually terminated. As such, we want to handle SIGINT and SIGTERM ourselves,
    and perform cleanup before exiting. This is achieved via signal masking. "Harsher" signals like SIGQUIT are left
    unhandled as there should be a way to end the program on the spot, if desired. */
    sigset_t terminationMask, originalMask, allSignals;
    if (sigemptyset(&terminationMask) != 0) {
        fprintf(stderr, "sigemptyset() error\n");
        return EXIT_FAILURE;
    }
    if (sigaddset(&terminationMask, SIGINT) != 0) {
        fprintf(stderr, "sigaddset() error (SIGINT)\n");
        return EXIT_FAILURE;
    }
    if (sigaddset(&terminationMask, SIGTERM) != 0) {
        fprintf(stderr, "sigaddset() error (SIGTERM)\n");
        return EXIT_FAILURE;
    }
    if (pthread_sigmask(SIG_BLOCK, &terminationMask, &originalMask) != 0) {
        fprintf(stderr, "pthread_sigmask() error (SIG_BLOCK)\n");
        return EXIT_FAILURE;
    }

    // One by one, initialize the threads' working data

    consumer_data consumerData = { .terminationFlag = FALSE };
    queueInit(&consumerData.fifo);

    // Set connection-attempt parameters, such as the time delay between each attempt, if the attempts should ever stop or continue until success, etc.
    const uint32_t backoff_ms[] = { 1000, 2000, 3000, 4000, 5000 };
    producer_data producerData = {
        .fifo = &consumerData.fifo,
        .terminationFlag = FALSE,
        .subscriptions = {"BTC-USDT", "ADA-USDT", "ETH-USDT", "DOGE-USDT", "XRP-USDT", "SOL-USDT", "LTC-USDT", "BNB-USDT"},
        .clientConnectData.retry.retry_ms_table = backoff_ms,
        .clientConnectData.retry.retry_ms_table_count = LWS_ARRAY_SIZE(backoff_ms),
        .clientConnectData.retry.conceal_count = LWS_RETRY_CONCEAL_ALWAYS,
        .clientConnectData.retry.secs_since_valid_ping = 3,
        .clientConnectData.retry.secs_since_valid_hangup = 10,
        .clientConnectData.retry.jitter_percent = 20
    };

    if (databaseInit(&consumerData.DB, producerData.subscriptions) != DATABASE_SUCCESS) {
        exitCode = EXIT_FAILURE;
        goto out_db;
    }

    per_interval_data perIntervalData = {
        .DB = &consumerData.DB,
        .terminationFlag = FALSE,
        .subs = &producerData.subscriptions
    };
    // The monotonic clock is independent of network time and as such, ideal for the internal timing of the "perInterval" thread
    pthread_condattr_t monotonic;
    pthread_condattr_init(&monotonic);
    if (pthread_condattr_setclock(&monotonic, CLOCK_MONOTONIC) != 0) {
        fprintf(stderr, "pthread_condattr_setclock() error\n");
        exitCode = EXIT_FAILURE;
        goto out_condattr;
    }
    pthread_cond_init(&perIntervalData.termination, &monotonic);

    // Libwebsockets connection initialization process
    // Set protocols struct
    const struct lws_protocols protocols[] = {
        {
            .name = "okx-protocol",
            .callback = callbackOKX,
            .per_session_data_size = 0,
            .rx_buffer_size = 0,
            .id = 0,
            .user = NULL,
            .tx_packet_size = 0
        },
        {
            .name = NULL,
            .callback = NULL,
            .per_session_data_size = 0,
            .rx_buffer_size = 0,
            .id = 1,
            .user = NULL,
            .tx_packet_size = 0
        } // terminator
    };

    // Create the libwebsockets context
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof(info));

    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.user = &producerData; // attach the "producer" data to the context, so that it can then be retrieved by calling "lws_context_user(context);"; used in the callbackOKX function and elsewhere

    struct lws_context *context = lws_create_context(&info);

    if (!context) {
        fprintf(stderr, "lws_init failed\n");
        exitCode = EXIT_FAILURE;
        goto out_lws;
    }

    // Set up connection parameters
    memset(&producerData.clientConnectData.info, 0, sizeof(producerData.clientConnectData.info));

    producerData.clientConnectData.info.context = context;
    producerData.clientConnectData.info.address = "ws.okx.com";
    producerData.clientConnectData.info.port = 8443;
    producerData.clientConnectData.info.path = "/ws/v5/public";
    producerData.clientConnectData.info.host = producerData.clientConnectData.info.address;
    producerData.clientConnectData.info.origin = producerData.clientConnectData.info.address;
    producerData.clientConnectData.info.protocol = protocols[0].name;
    producerData.clientConnectData.info.ssl_connection = LCCSCF_USE_SSL;
    producerData.clientConnectData.info.retry_and_idle_policy = &producerData.clientConnectData.retry;
    producerData.clientConnectData.info.pwsi = &producerData.clientConnectData.wsi;

    // Initiate connection
    lws_sul_schedule(context, 0, &producerData.clientConnectData.sul, connectClient, 1); // see the connectClient function for more details

 /* The threads we create will inherit the signal mask of the creator.
    Since we want all signal handling to be done by the otherwise idle main thread,
    we temporarily set a signal mask that blocks all signals,
    only to then restore the previous one after all threads have been created.
    This way no thread except 'main' will respond to signals. */
    if (sigfillset(&allSignals) != 0) {
        fprintf(stderr, "sigfillset() error\n");
        exitCode = EXIT_FAILURE;
        goto out_sig;
    }
    if (pthread_sigmask(SIG_SETMASK, &allSignals, &terminationMask) != 0) {
        fprintf(stderr, "pthread_sigmask() error (allSignals)\n");
        exitCode = EXIT_FAILURE;
        goto out_sig;
    }

    // Create threads
    thread_info threadsInfo[THREADS] = {
        {
            .mut = &producerData.fifo->mut,
            .cond = &producerData.fifo->notFull,
            .flag = &producerData.terminationFlag,
            .name = "producer",
            .func = producer,
            .args = context
        },
        {
            .mut = &consumerData.fifo.mut,
            .cond = &consumerData.fifo.notEmpty,
            .flag = &consumerData.terminationFlag,
            .name = "consumer",
            .func = consumer,
            .args = &consumerData
        },
        {
            .mut = &perIntervalData.DB->mut,
            .cond = &perIntervalData.termination,
            .flag = &perIntervalData.terminationFlag,
            .name = "per-interval",
            .func = perInterval,
            .args = &perIntervalData
        }
    };

    int n;
    for (n = 0; n < THREADS; ++n) {
        if (pthread_create(&threadsInfo[n].id, NULL, threadsInfo[n].func, threadsInfo[n].args) != 0) {
            fprintf(stderr, "pthread_create() error: failed to create %s thread\n", threadsInfo[n].name);
            exitCode = EXIT_FAILURE;
            goto out_full_cleanup;
        }
    }

    // Restore signal mask
    if (pthread_sigmask(SIG_SETMASK, &terminationMask, NULL) != 0) {
        fprintf(stderr, "pthread_sigmask() error: failed to restore terminationMask\n");
        exitCode = EXIT_FAILURE;
        goto out_full_cleanup;
    }

    // Wait for program shutdown signal
    n = 0;
    while (n != SIGINT && n != SIGTERM) {
        sigwait(&terminationMask, &n);
    }
    printf("\nCaught \"%s\" signal, shutting down..\n", strsignal(n));

    n = THREADS;
    out_full_cleanup:
    // Restore original signal mask. This way, if the program hangs, SIGINT/SIGTERM can be raised again to interrupt it in the normal way.
    if (pthread_sigmask(SIG_SETMASK, &originalMask, NULL) != 0) {
        fprintf(stderr, "pthread_sigmask() error: failed to restore original signal mask\n");
        exitCode = EXIT_FAILURE;
    }

    // Shutdown the threads one by one
    while (n--) {
        pthread_mutex_lock(threadsInfo[n].mut);
        *threadsInfo[n].flag = TRUE;
        pthread_cond_signal(threadsInfo[n].cond);
        pthread_mutex_unlock(threadsInfo[n].mut);

        if (pthread_join(threadsInfo[n].id, NULL) != 0) {
            fprintf(stderr, "pthread_join() error: failed to join %s thread\n", threadsInfo[n].name);
            exitCode = EXIT_FAILURE;
        }
        else {
            printf("Joined %s thread.\n", threadsInfo[n].name);
        }
    }

    out_sig:
    lws_context_destroy(context);

    out_lws:
    if (pthread_cond_destroy(&perIntervalData.termination) != 0) {
        fprintf(stderr, "pthread_cond_destroy() error: failed to destroy termination condition\n");
    }

    out_condattr:
    pthread_condattr_destroy(&monotonic);
    databaseDelete(&consumerData.DB);

    out_db:
    queueDelete(&consumerData.fifo);

    printf("Cleaned up.\n");

    return exitCode;
}

static int strtoi(const char *s) {
    long x = strtol(s, NULL, 10);
    if (x < INT_MIN || x > INT_MAX) {
        return 0;
    }
    return (int)x;
}

static unsigned long long unixTimeInMs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (unsigned long long)(ts.tv_sec)*1000 + (unsigned long long)(ts.tv_nsec)/1000000;
}

static double pearsonCorrCoeff(int n, double X[n], double Y[n]) {
    double Xbar = 0, Ybar = 0, covXY = 0, stdDevX = 0, stdDevY = 0;
    int i = 0, notNaNcount = 0;

    for (i = 0; i < n; ++i) {
        if (!isnan(X[i]) && !isnan(Y[i])) {
            Xbar += X[i];
            Ybar += Y[i];
            ++notNaNcount;
        }
    }

    if (notNaNcount <= 1) {
        return NAN;
    }

    Xbar = Xbar / notNaNcount;
    Ybar = Ybar / notNaNcount;
    for (i = 0; i < n; ++i) {
        if (!isnan(X[i]) && !isnan(Y[i])) {
            covXY += (X[i] - Xbar) * (Y[i] - Ybar);
            stdDevX += pow(X[i] - Xbar, 2);
            stdDevY += pow(Y[i] - Ybar, 2);
        }
    }
    if (stdDevX == 0 || stdDevY == 0) {
        return NAN;
    }
    return covXY / sqrt(stdDevX*stdDevY);
}

static int checkPearsonMax(pearson_corr pearson, pearson_corr *maxPearson, int currentBestIndicator, int instrumentBeingChecked) {
    if (!isnan(pearson.coeff) && fabs(maxPearson->coeff) < fabs(pearson.coeff)) {
        maxPearson->coeff = pearson.coeff;
        maxPearson->corrTime = pearson.corrTime;
        return instrumentBeingChecked;
    }
    return currentBestIndicator;
}

static enum db_status databaseInit(database *db, const char subsArray[SUBSCRIPTIONS][SUBSIZE]) {
    enum db_status ret = DATABASE_SUCCESS;

    int i, j, metricIndex, openFilesPerMetric[METRICS];
    for (i = 0; i < SUBSCRIPTIONS; ++i) {
        db->sumPrices[i] = 0;
        db->totalSize[i] = 0;
        db->numTrades[i] = 0;
    }

    const char extension[] = ".txt";
    const char* const headers[METRICS] = {"Price,Size,Timestamp,TimeReceived,TimeSaved", "AveragePrice,TotalSize,Timestamp",
                             "BestIndicator,Coefficient,CorrelationTime,Timestamp"};

    const char* const filepaths[METRICS+1] = {"generated-files/", "generated-files/trade-data/", "generated-files/moving-averages/",
                               "generated-files/pearson-correlation-coefficients/"};

    struct stat st = {0};
    for (i = 0; i <= METRICS; ++i) {
        if (stat(filepaths[i], &st) == -1) {
            if (mkdir(filepaths[i], 0777) != 0) {
                fprintf(stderr, "mkdir() error\n");
                return MKDIR_ERROR;
            }
        }
    }

    for (i = 1; i <= METRICS; ++i) {
        metricIndex = i-1; // compensating for the fact that there is an extra file path, due to the need for a parent directory "generated-files/"
        openFilesPerMetric[metricIndex] = 0;
        for (j = 0; j < SUBSCRIPTIONS; ++j) {
            char fileSpec[strlen(filepaths[i]) + strlen(subsArray[j]) + sizeof(extension)];
            if (snprintf(fileSpec, sizeof(fileSpec), "%s%s%s", filepaths[i], subsArray[j], extension) < 0) {
                fprintf(stderr, "snprintf() error: failed to print into fileSpec buffer\n");
                ret = SNPRINTF_ERROR;
                goto out_error;
            }
            if (metricIndex == MOVAVG) {
                db->files[metricIndex][j] = fopen(fileSpec, "w+"); // moving average files will also need to be read
            }
            else {
                db->files[metricIndex][j] = fopen(fileSpec, "w");
            }
            if (db->files[metricIndex][j] == NULL) {
                fprintf(stderr, "fopen() error\n");
                ret = FOPEN_ERROR;
                goto out_error;
            }
            ++openFilesPerMetric[metricIndex];
            if (fprintf(db->files[metricIndex][j], "%s\n", headers[metricIndex]) < 0) {
                fprintf(stderr, "fprintf() error: failed to print headers\n");
                ret = FPRINTF_ERROR;
                goto out_error;
            }
        }
    }
    pthread_mutex_init(&db->mut, NULL);
    goto out;

    out_error:
    for (i = 0; i <= metricIndex; ++i) {
        for (j = 0; j < openFilesPerMetric[i]; ++j) {
            if (fclose(db->files[i][j]) != 0) {
                fprintf(stderr, "fclose() error\n");
            }
        }
    }

    out:
    return ret;
}

static void databaseDelete(database *db) {
    int i, j;
    for (i = 0; i < METRICS; ++i) {
        for (j = 0; j < SUBSCRIPTIONS; ++j) {
            if (fclose(db->files[i][j]) != 0) {
                fprintf(stderr, "fclose() error\n");
            }
        }
    }
    if (pthread_mutex_destroy(&db->mut) != 0) {
        fprintf(stderr,"pthread_mutex_destroy() error: failed to destroy database mutex\n");
    }
}

// This method of establishing a WebSocket connection is taken from the LibWebSockets API documentation
// See https://libwebsockets.org/git/libwebsockets/tree/minimal-examples-lowlevel/ws-client/minimal-ws-client/minimal-ws-client.c
static void connectClient(lws_sorted_usec_list_t *sul) {
    client_connect_data *clientConnectData = lws_container_of(sul, client_connect_data, sul);

    if (!lws_client_connect_via_info(&clientConnectData->info)) {
        lws_retry_sul_schedule(clientConnectData->info.context, 0, sul, &clientConnectData->retry, connectClient,
                               &clientConnectData->retry_count);
    }
}

// The producer thread handles the WebSocket events
static void *producer (void *lwsContextStruct) {
    struct lws_context *context = lwsContextStruct;
    producer_data *producerData = lws_context_user(context);

    while (!producerData->terminationFlag) {
        lws_service(context, 0);
    }
    return NULL;
}

// The consumer thread takes the trade data in the queue and saves it
static void *consumer (void *consumerDataStruct) {
    consumer_data *consumerData = consumerDataStruct;
    trade t;

    while (!consumerData->terminationFlag) {
        pthread_mutex_lock(&consumerData->fifo.mut); // protect against conflict with producer thread
        while (consumerData->fifo.empty && !consumerData->terminationFlag) { // protects against spurious wake-up
            //printf("consumer: queue EMPTY.\n");
            pthread_cond_wait(&consumerData->fifo.notEmpty, &consumerData->fifo.mut);
        }
        // Non-spurious wake-up + queue is empty = program has initiated shutdown
        if (consumerData->fifo.empty) {
            pthread_mutex_unlock(&consumerData->fifo.mut);
            return NULL;
        }
        queueDel(&consumerData->fifo, &t);
        pthread_cond_signal(&consumerData->fifo.notFull);
        pthread_mutex_unlock(&consumerData->fifo.mut); // after removing the trade from the queue, there is no need to hold on to the queue mutex anymore

        pthread_mutex_lock(&consumerData->DB.mut); // protect against conflict with perInterval thread
        // Add the trade's price/size to the interval's data and increment the interval's trades counter [for this instrument]
        consumerData->DB.sumPrices[t.subIndex] += t.price*t.count; // unlike the size data, the price data is not aggregated by default (in the case of multiple trades being aggregated in one message)
        consumerData->DB.totalSize[t.subIndex] += t.size;
        consumerData->DB.numTrades[t.subIndex] += t.count;
        pthread_mutex_unlock(&consumerData->DB.mut);

        // No other thread writes to the raw trade data files, so no need to hold on to the mutex while writing to them
        fprintf(consumerData->DB.files[TRADE][t.subIndex], "%lf,%lf,%llu,%llu,%llu\n", t.price, t.size, t.timestamp, t.timeReceived, unixTimeInMs());
    }
    return NULL;
}

// The "perInterval" thread wakes once per defined interval to perform calculations on the collected data
static void *perInterval (void *perIntervalDataStruct) {
    struct timespec ts; // time struct that the thread will be looking at to see when it should wake up
    while (clock_gettime(CLOCK_MONOTONIC, &ts) != 0); // monotonic is the better choice for internal timings due to independence from the network
    ts.tv_sec += INTERVAL; // this makes the time struct "point" one INTERVAL into the future

    per_interval_data *perIntervalData = perIntervalDataStruct;

    pearson_corr pearson, maxPearson;

    // Declaration and initialization of variables relating to <INTERVALS>-window calculations
    double sumPrices[SUBSCRIPTIONS][INTERVALS], totalSize[SUBSCRIPTIONS][INTERVALS], priceAccum, sizeAccum,
           priceAvg[SUBSCRIPTIONS][DATAPOINTS], slidingAvgVector[DATAPOINTS];

    int i, j, k, numTrades[SUBSCRIPTIONS][INTERVALS], tradesAccum, bestIndicator, intervalCount = 0, elapsedIntervals = 0;
    char line[MAX_LINE+1];

    for (i = 0; i < SUBSCRIPTIONS; ++i) {
        for (j = 0; j < INTERVALS; ++j) {
            sumPrices[i][j] = 0;
            totalSize[i][j] = 0;
            numTrades[i][j] = 0;
        }
        for (j = 0; j < DATAPOINTS; ++j) {
            priceAvg[i][j] = NAN;
        }
    }
    for (i = 0; i < DATAPOINTS; ++i) {
        slidingAvgVector[i] = NAN;
    }

    while (!perIntervalData->terminationFlag) {
        pthread_mutex_lock(&perIntervalData->DB->mut);

        // Unless the timer runs out or program shutdown is initiated, wait
        while (!perIntervalData->terminationFlag &&
                pthread_cond_timedwait(&perIntervalData->termination, &perIntervalData->DB->mut, &ts) != ETIMEDOUT);
        // Upon non-spurious wake-up, *immediately* set the time struct one INTERVAL into the future, so that no "drift" occurs due to processing delays
        ts.tv_sec += INTERVAL;

        // If the wake-up was due to program shutdown initiation, don't do the processing - most likely the timer is not near the end of an interval anyway
        if (perIntervalData->terminationFlag) {
            pthread_mutex_unlock(&perIntervalData->DB->mut);
            return NULL;
        }
        // For every instrument
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            // Fetch the data of the latest interval
            sumPrices[i][intervalCount] = perIntervalData->DB->sumPrices[i];
            totalSize[i][intervalCount] = perIntervalData->DB->totalSize[i];
            numTrades[i][intervalCount] = perIntervalData->DB->numTrades[i];
            // Reset the arrays that it got fetched from, so that they record the next interval
            perIntervalData->DB->sumPrices[i] = 0;
            perIntervalData->DB->totalSize[i] = 0;
            perIntervalData->DB->numTrades[i] = 0;
        }
        // No longer need the mutex, as the only shared resources are sumPrices, totalSize and numTrades, and we are done with those
        pthread_mutex_unlock(&perIntervalData->DB->mut);

        // Calculation of simple moving average and total size for the latest <INTERVALS> window
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            priceAccum = 0;
            sizeAccum = 0;
            tradesAccum = 0;
            for (j = 0; j < INTERVALS; ++j) {
                priceAccum += sumPrices[i][j];
                sizeAccum += totalSize[i][j];
                tradesAccum += numTrades[i][j];
            }
            /* Each instrument has a <DATAPOINTS>-long "average price" vector (updated every interval) stored for Pearson correlation coefficient
               calculations. For the first <DATAPOINTS> intervals, this vector is simply filled in order. */
            if (elapsedIntervals < DATAPOINTS) {
                if (tradesAccum != 0) {
                    priceAvg[i][elapsedIntervals] = priceAccum / tradesAccum;
                }
                else {
                    priceAvg[i][elapsedIntervals] = NAN;
                }
                // No other thread ever accesses these files so no need to protect access with a mutex
                fprintf(perIntervalData->DB->files[MOVAVG][i], "%lf,%lf,%llu\n", priceAvg[i][elapsedIntervals], sizeAccum, unixTimeInMs());
            }
            /* After the first <DATAPOINTS> intervals, every element of the vector is first shifted one spot towards the "head", while the "head" is
               discarded. The newest entry is added to the "tail". This is necessary to make sure the vector is always aligned oldest-to-newest,
               which is required for some edge cases (relating to potential NaN entries, which can happen if e.g. no data comes for an entire INTERVAL).*/
            else {
                memmove(&priceAvg[i][0], &priceAvg[i][1], (DATAPOINTS-1) * sizeof(priceAvg[i][0]));
                if (tradesAccum != 0) {
                    priceAvg[i][DATAPOINTS-1] = priceAccum / tradesAccum;
                }
                else {
                    priceAvg[i][DATAPOINTS-1] = NAN;
                }
                // No other thread ever accesses these files so no need to protect access with a mutex
                fprintf(perIntervalData->DB->files[MOVAVG][i], "%lf,%lf,%llu\n", priceAvg[i][DATAPOINTS-1], sizeAccum, unixTimeInMs());
            }
        }
        ++elapsedIntervals;

        /* Calculation of Maximum Pearson Correlation Coefficient: for each instrument, what is computed is the coefficient of its latest
           "average price" vector with all past (and current, if applicable) "average price" vectors of every other instrument (and itself),
           in order. The largest coefficient found is then saved as this interval's coefficient: its value, how far back in time it was found,
           and the instrument it was found with. */
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            bestIndicator = UNDEFINED;
            maxPearson.coeff = 0;
            maxPearson.corrTime = UNDEFINED;
            for (j = 0; j < SUBSCRIPTIONS; ++j) {
                if (elapsedIntervals < 2*DATAPOINTS && i == j) {
                    continue;
                }
                rewind(perIntervalData->DB->files[MOVAVG][j]); // go to the start of the moving average file
                if (!fgets(line, MAX_LINE, perIntervalData->DB->files[MOVAVG][j])) { // skip the headers
                    continue;
                }

                // Get the first <DATAPOINTS> moving averages of the file. This is the first vector to compute the coefficient with.
                // If <DATAPOINTS> intervals have not passed yet, compute it with whatever values there are.
                for (k = 0; k < DATAPOINTS && fgets(line, MAX_LINE, perIntervalData->DB->files[MOVAVG][j]) != NULL; ++k) {
                    sscanf(line,"%lf,%*f,%llu\n", &slidingAvgVector[k], &pearson.corrTime);
                }
                pearson.coeff = pearsonCorrCoeff(DATAPOINTS, priceAvg[i], slidingAvgVector);
                // Check if the computed coefficient is the maximum so far and note down its details if it is.
                bestIndicator = checkPearsonMax(pearson, &maxPearson, bestIndicator, j);

                // Next, move down the file line-by-line and compute the new coefficient each time. Stop at <DATAPOINTS> lines before the end of the file
                for ( ; k < elapsedIntervals-DATAPOINTS && fgets(line, MAX_LINE, perIntervalData->DB->files[MOVAVG][j]) != NULL; ++k) {
                    memmove(&slidingAvgVector[0], &slidingAvgVector[1], (DATAPOINTS-1) * sizeof(slidingAvgVector[0])); // again, for alignment purposes
                    sscanf(line,"%lf,%*f,%llu\n", &slidingAvgVector[DATAPOINTS-1], &pearson.corrTime);

                    pearson.coeff = pearsonCorrCoeff(DATAPOINTS, priceAvg[i], slidingAvgVector);
                    // Don't forget to check if it's the maximum so far, each time
                    bestIndicator = checkPearsonMax(pearson, &maxPearson, bestIndicator, j);
                }
                // The final <DATAPOINTS> lines are only taken into consideration if the current instrument is not being compared to its own past
                if (i != j) {
                    while (fgets(line, MAX_LINE, perIntervalData->DB->files[MOVAVG][j]) != NULL) {
                        memmove(&slidingAvgVector[0], &slidingAvgVector[1], (DATAPOINTS-1) * sizeof(slidingAvgVector[0]));
                        sscanf(line,"%lf,%*f,%llu\n", &slidingAvgVector[DATAPOINTS-1], &pearson.corrTime);

                        pearson.coeff = pearsonCorrCoeff(DATAPOINTS, priceAvg[i], slidingAvgVector);
                        bestIndicator = checkPearsonMax(pearson, &maxPearson, bestIndicator, j);
                    }
                }
            }
            // Save coefficients to their respective files; again, no need for a mutex here
            if (bestIndicator != UNDEFINED) {
                fprintf(perIntervalData->DB->files[PEARSON][i], "%s,%lf,%llu,%llu\n", (*perIntervalData->subs)[bestIndicator],
                                                                                        maxPearson.coeff,
                                                                                        maxPearson.corrTime,
                                                                                        unixTimeInMs());
            }
            else {
                fprintf(perIntervalData->DB->files[PEARSON][i], "%lf,%lf,%lf,%llu\n", NAN,
                                                                                      NAN,
                                                                                      NAN,
                                                                                      unixTimeInMs());
            }
        }
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            if (ferror(perIntervalData->DB->files[MOVAVG][i]) != 0) {
                fseek(perIntervalData->DB->files[MOVAVG][i], 0, SEEK_END);
            }
        }
        // A kind of simplified "circular buffer"
        if (++intervalCount == INTERVALS) {
            intervalCount = 0;
        }
    }
    return NULL;
}

static void queueInit(queue *q) {
    q->empty = 1;
    q->full = 0;
    q->head = 0;
    q->tail = 0;
    pthread_mutex_init(&q->mut, NULL);
    pthread_cond_init(&q->notFull, NULL);
    pthread_cond_init(&q->notEmpty, NULL);
}

static void queueDelete(queue *q) {
    if (pthread_mutex_destroy(&q->mut) != 0) {
        fprintf(stderr, "pthread_mutex_destroy() error: failed to destroy queue mutex\n");
    }
    if (pthread_cond_destroy(&q->notFull) != 0) {
        fprintf(stderr, "pthread_cond_destroy() error: failed to destroy notFull condition\n");
    }
    if (pthread_cond_destroy(&q->notEmpty) != 0) {
        fprintf(stderr, "pthread_cond_destroy() error: failed to destroy notEmpty condition\n");
    }
}

static void queueAdd(queue *q, trade in) {
    q->buf[q->tail] = in;
    q->tail++;
    if (q->tail == QUEUESIZE) {
        q->tail = 0;
    }
    if (q->tail == q->head) {
        q->full = 1;
    }
    q->empty = 0;
}

static void queueDel(queue *q, trade *out) {
    *out = q->buf[q->head];
    q->head++;
    if (q->head == QUEUESIZE) {
        q->head = 0;
    }
    if (q->head == q->tail) {
        q->empty = 1;
    }
    q->full = 0;
}
