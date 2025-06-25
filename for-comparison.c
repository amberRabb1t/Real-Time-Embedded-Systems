#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <libwebsockets.h>
#include <jansson.h>

#define QUEUESIZE 16384     // size of the fifo queue used for the incoming websocket data
#define METRICS 3           // number of metrics we'll be recording (including raw trade data), each corresponding to a file
#define SUBSCRIPTIONS 8     // number of instruments we'll be tracking
#define SUBSIZE 10          // maximum string size of the instrument IDs we'll be subscribing to
#define INTERVAL 60         // time interval for the "everyInterval" thread, in seconds
#define INTERVALS 15        // number of intervals over which certain metrics are calculated
#define DATAPOINTS 8        // number of datapoints kept for pearson correlation calculation
#define EXTENSION ".txt"    // file extension for the files that will save the data
#define MAX_LINE 128        // buffer size for reading moving-average-file lines

typedef struct {
    int subIndex, count;
    double price, size;
    unsigned long long timestamp, timeReceived;
} trade;

typedef struct {
    double coeff;
    unsigned long long corrTime;
} pearsonCorr;

typedef struct {
    FILE* files[METRICS][SUBSCRIPTIONS];
    double sumPrices[SUBSCRIPTIONS], totalSize[SUBSCRIPTIONS];
    int numTrades[SUBSCRIPTIONS];
    pthread_mutex_t mut;
} database;

typedef struct {
    trade buf[QUEUESIZE];
    long head, tail;
    int full, empty;
    pthread_mutex_t mut;
    pthread_cond_t notFull, notEmpty;
} queue;

typedef struct {
    lws_sorted_usec_list_t sul;
    struct lws *wsi;
    struct lws_client_connect_info ccinfo;
    lws_retry_bo_t retry;
    uint16_t retry_count;
} clientConnectData;

typedef struct {
    clientConnectData ccDat;
    queue *fifo;
    int interrupted, subCount;
    const char* const subscriptions[SUBSCRIPTIONS];
    char subList[SUBSCRIPTIONS][SUBSIZE];
} producerData;

typedef struct {
    database DB;
    queue fifo;
    int consumerFlag;
} consumerData;

typedef struct {
    database *DB;
    int finish;
    const char* const (*subs)[SUBSCRIPTIONS];
    pthread_cond_t finishUp;
} intervalData;

static unsigned long long unixTimeInMs();
static double pearsonCorrCoeff(int n, double X[n], double Y[n]);

static void databaseInit(database *db, const char* const subsArr[SUBSCRIPTIONS]);
static void databaseDelete(database *db);
static void connect_client(lws_sorted_usec_list_t *sul);

static void *producer(void *args);
static void *consumer(void *args);
static void *everyInterval(void *args);

static void queueInit(queue *q);
static void queueDelete(queue *q);
static void queueAdd(queue *q, trade in);
static void queueDel(queue *q, trade *out);

// Callback function that processes WebSocket events
static int callback_okx(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len) {
    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED: {
            printf("\nWebSocket connection established\n");
            producerData *proDat = lws_context_user(lws_get_context(wsi));
            proDat->subCount = 0;
            for (int i = 0; i < SUBSCRIPTIONS; ++i) {
                strncpy(proDat->subList[i], proDat->subscriptions[i], SUBSIZE);
            }
            lws_callback_on_writable(wsi);
            return 0;
        }

        case LWS_CALLBACK_CLIENT_WRITEABLE: {
            producerData *proDat = lws_context_user(lws_get_context(wsi));
            if (proDat->subCount < SUBSCRIPTIONS) {
                char subSpec[strlen("{\"op\":\"subscribe\",\"args\": [{\"channel\": \"trades\",\"instId\": \"\"}]}") +
                             strlen(proDat->subList[proDat->subCount]) + 1];
                snprintf(subSpec, sizeof(subSpec), "{\"op\":\"subscribe\",\"args\": [{\"channel\": \"trades\",\"instId\": \"%s\"}]}",
                         proDat->subList[proDat->subCount]);

                unsigned char buf[LWS_SEND_BUFFER_PRE_PADDING + strlen(subSpec) + LWS_SEND_BUFFER_POST_PADDING];
                memset(buf, 0, sizeof(buf));
                memcpy(&buf[LWS_SEND_BUFFER_PRE_PADDING], subSpec, strlen(subSpec));

                //printf("Sending: %s\n", subSpec);
                lws_write(wsi, &buf[LWS_SEND_BUFFER_PRE_PADDING], strlen(subSpec), LWS_WRITE_TEXT);
            }
            return 0;
        }

        case LWS_CALLBACK_CLIENT_RECEIVE: {
            if (strcmp((const char*)in, "ping") == 0) {
                printf("Received:  Ping\n");
                return 0;
            }

            json_t *root;
            json_error_t error;

            root = json_loads((const char*)in, 0, &error);

            if (!root) {
                fprintf(stderr, "Error: on line %d: %s\n", error.line, error.text);
                json_decref(root);
                return 1;
            }

            json_t *type = json_object_get(root, "event");

            if (type != NULL) {
                const char *type_value = json_string_value(type);

                if (strcmp(type_value, "error") == 0) {
                    printf("Received:  Error\n");
                    json_decref(root);
                    return 1;
                }
                producerData *proDat = lws_context_user(lws_get_context(wsi));

                if (strcmp(type_value, "subscribe") == 0) {
                    json_t *sub = json_object_get(root, "arg");

                    printf("Received:   Event: %s\tSymbol: %s\tChannel: %s\t\tConnectionID: %s\n",
                            type_value,
                            (const char *)json_string_value(json_object_get(sub, "instId")),
                            (const char *)json_string_value(json_object_get(sub, "channel")),
                            (const char *)json_string_value(json_object_get(root, "connId")));

                    if (++proDat->subCount < SUBSCRIPTIONS) {
                        lws_callback_on_writable(wsi);
                    }
                }
                else if (strcmp(type_value, "unsubscribe") == 0) {
                    json_t *sub = json_object_get(root, "arg");
                    const char *instId = (const char *)json_string_value(json_object_get(sub, "instId"));

                    printf("Received:   Event: %s\tSymbol: %s\tChannel: %s\t\tConnectionID: %s\n",
                            type_value,
                            instId,
                            (const char *)json_string_value(json_object_get(sub, "channel")),
                            (const char *)json_string_value(json_object_get(root, "connId")));

                    strncpy(proDat->subList[--proDat->subCount], instId, SUBSIZE);
                    lws_callback_on_writable(wsi);
                }
            }

            else {
                json_t *data = json_object_get(root, "data");

                if (data != NULL) {
                    json_t *metrics = json_array_get(data, 0);

                    json_t *instId = json_object_get(metrics, "instId");
                    const char *instId_value = (const char *)json_string_value(instId);

                    json_t *cnt_obj = json_object_get(metrics, "count");
                    json_t *px_obj = json_object_get(metrics, "px");
                    json_t *ts_obj = json_object_get(metrics, "ts");
                    json_t *sz_obj = json_object_get(metrics, "sz");

                    producerData *proDat = lws_context_user(lws_get_context(wsi));

                    for (int i = 0; i < SUBSCRIPTIONS; ++i) {
                        if (strcmp(instId_value, proDat->subscriptions[i]) == 0) {
                            trade currentTrade;

                            currentTrade.subIndex = i;
                            currentTrade.count = strtod((const char *)json_string_value(cnt_obj), NULL);
                            currentTrade.price = strtod((const char *)json_string_value(px_obj), NULL);
                            currentTrade.size = strtod((const char *)json_string_value(sz_obj), NULL);
                            currentTrade.timestamp = strtoull((const char *)json_string_value(ts_obj), NULL, 10);
                            currentTrade.timeReceived = unixTimeInMs();

                            pthread_mutex_lock(&proDat->fifo->mut);
                            while (proDat->fifo->full && !proDat->interrupted) {
                                printf("producer: queue FULL.\n");
                                pthread_cond_wait(&proDat->fifo->notFull, &proDat->fifo->mut);
                            }
                            if (proDat->fifo->full) {
                                pthread_mutex_unlock(&proDat->fifo->mut);
                                json_decref(root);
                                return 0;
                            }
                            queueAdd(proDat->fifo, currentTrade);
                            pthread_cond_signal(&proDat->fifo->notEmpty);
                            pthread_mutex_unlock(&proDat->fifo->mut);
                            json_decref(root);
                            return 0;
                        }
                    }
                }
                else {
                    printf("Received null data.\n");
                }
            }
            json_decref(root);
            return 0;
        }

        case LWS_CALLBACK_CLIENT_CLOSED: {
            printf("WebSocket connection closed\n");
            producerData *proDat = lws_context_user(lws_get_context(wsi));
            if (!proDat->interrupted) {
                lws_retry_sul_schedule_retry_wsi(wsi, &proDat->ccDat.sul, connect_client, &proDat->ccDat.retry_count);
            }
            return 0;
        }

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR: {
            fprintf(stderr, "WebSocket connection error\n");
            producerData *proDat = lws_context_user(lws_get_context(wsi));
            if (!proDat->interrupted) {
                lws_retry_sul_schedule_retry_wsi(wsi, &proDat->ccDat.sul, connect_client, &proDat->ccDat.retry_count);
            }
            return 0;
        }

        default: {
            return 0;
        }
    }

    return 0;
}

int main() {
/*  The program is designed to run until manually terminated. As such, we want to handle SIGINT and SIGTERM ourselves, and perform cleanup
    before exiting. "Harsher" signals like SIGQUIT are left unhandled as there should be a way to end the program on the spot, if desired. */
    sigset_t terminationMask, originalMask, allSignals;
    sigemptyset(&terminationMask);
    sigaddset(&terminationMask, SIGINT);
    sigaddset(&terminationMask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &terminationMask, &originalMask);

    // Initialize the threads' working data
    consumerData conDat = { .consumerFlag = 0 };
    queueInit(&conDat.fifo);

    const uint32_t backoff_ms[] = { 1000, 2000, 3000, 4000, 5000 };
    producerData proDat = {
        .fifo = &conDat.fifo,
        .interrupted = 0,
        .subscriptions = {"BTC-USDT", "ADA-USDT", "ETH-USDT", "DOGE-USDT", "XRP-USDT", "SOL-USDT", "LTC-USDT", "BNB-USDT"},
        .ccDat.retry.retry_ms_table = backoff_ms,
        .ccDat.retry.retry_ms_table_count = LWS_ARRAY_SIZE(backoff_ms),
        .ccDat.retry.conceal_count = LWS_RETRY_CONCEAL_ALWAYS,
        .ccDat.retry.secs_since_valid_ping = 3,
        .ccDat.retry.secs_since_valid_hangup = 10,
        .ccDat.retry.jitter_percent = 20
    };

    databaseInit(&conDat.DB, proDat.subscriptions);

    intervalData intDat = {
        .DB = &conDat.DB,
        .finish = 0,
        .subs = &proDat.subscriptions
    };
    pthread_condattr_t monotonic;
    pthread_condattr_init(&monotonic);
    pthread_condattr_setclock(&monotonic, CLOCK_MONOTONIC);
    pthread_cond_init(&intDat.finishUp, &monotonic);

    // Libwebsockets connection initialization process
    // Set protocols struct
    const struct lws_protocols protocols[] = {
        {
            "okx-protocol",
            callback_okx,
            0, 0
        },
        { NULL, NULL, 0, 0 } // terminator
    };

    // Create the libwebsockets context
    struct lws_context_creation_info info;
    memset(&info, 0, sizeof info);

    info.port = CONTEXT_PORT_NO_LISTEN;
    info.protocols = protocols;
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.user = &proDat;

    struct lws_context *context = lws_create_context(&info);

    if (!context) {
        fprintf(stderr, "lws_init failed\n");
        pthread_cond_destroy(&intDat.finishUp);
        pthread_condattr_destroy(&monotonic);
        databaseDelete(&conDat.DB);
        queueDelete(&conDat.fifo);
        return 1;
    }

    // Set up connection parameters
    memset(&proDat.ccDat.ccinfo, 0, sizeof(proDat.ccDat.ccinfo));

    proDat.ccDat.ccinfo.context = context;
    proDat.ccDat.ccinfo.address = "ws.okx.com";
    proDat.ccDat.ccinfo.port = 8443;
    proDat.ccDat.ccinfo.path = "/ws/v5/public";
    proDat.ccDat.ccinfo.host = proDat.ccDat.ccinfo.address;
    proDat.ccDat.ccinfo.origin = proDat.ccDat.ccinfo.address;
    proDat.ccDat.ccinfo.protocol = protocols[0].name;
    proDat.ccDat.ccinfo.ssl_connection = LCCSCF_USE_SSL;
    proDat.ccDat.ccinfo.retry_and_idle_policy = &proDat.ccDat.retry;
    proDat.ccDat.ccinfo.pwsi = &proDat.ccDat.wsi;

    // Initiate connection
    lws_sul_schedule(context, 0, &proDat.ccDat.sul, connect_client, 1);

 /* The threads we create will inherit the signal mask of the creator.
    Since we want all signal handling to be done by the otherwise idle main thread,
    we temporarily set a signal mask that blocks all signals,
    only to then restore the previous one after all threads have been created.
    This way no thread except 'main' will handle signals. */
    sigfillset(&allSignals);
    pthread_sigmask(SIG_SETMASK, &allSignals, &terminationMask);

    // Create threads
    pthread_t pro, con, perInt;
    pthread_create(&pro, NULL, producer, context);
    pthread_create(&perInt, NULL, everyInterval, &intDat);
    pthread_create(&con, NULL, consumer, &conDat);

    // Restore signal mask
    pthread_sigmask(SIG_SETMASK, &terminationMask, NULL);

    // Wait for program shutdown signal
    int sig = 0;
    while (sig != SIGINT && sig != SIGTERM) {
        sigwait(&terminationMask, &sig);
    }

    printf("\nCaught SIGINT, shutting down..\n");

    // Restore original signal mask. This way, if the program hangs, SIGINT/SIGTERM can be raised again to interrupt it in the normal way.
    pthread_sigmask(SIG_SETMASK, &originalMask, NULL);

    // Shutdown producer thread
    pthread_mutex_lock(&conDat.fifo.mut);
    proDat.interrupted = 1;
    pthread_cond_signal(&conDat.fifo.notFull);
    pthread_mutex_unlock(&conDat.fifo.mut);

    pthread_join(pro, NULL);
    printf("Joined producer thread.\n");

    // Shutdown consumer thread
    pthread_mutex_lock(&conDat.fifo.mut);
    conDat.consumerFlag = 1;
    pthread_cond_signal(&conDat.fifo.notEmpty);
    pthread_mutex_unlock(&conDat.fifo.mut);

    pthread_join(con, NULL);
    printf("Joined consumer thread.\n");

    // Shutdown per-interval thread
    pthread_mutex_lock(&conDat.DB.mut);
    intDat.finish = 1;
    pthread_cond_signal(&intDat.finishUp);
    pthread_mutex_unlock(&conDat.DB.mut);

    pthread_join(perInt, NULL);
    printf("Joined per-interval thread.\n");

    // Cleanup
    lws_context_destroy(context);
    pthread_cond_destroy(&intDat.finishUp);
    pthread_condattr_destroy(&monotonic);
    databaseDelete(&conDat.DB);
    queueDelete(&conDat.fifo);

    printf("Cleaned up.\n");

    return 0;
}

static unsigned long long unixTimeInMs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    return (unsigned long long)(ts.tv_sec) * 1000 + (unsigned long long)(ts.tv_nsec) / 1000000;
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

    if (notNaNcount > 1) {
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
        else {
            return covXY / sqrt(stdDevX*stdDevY);
        }
    }
    else {
        return NAN;
    }
}

static void databaseInit(database *db, const char* const subsArr[SUBSCRIPTIONS]) {
    pthread_mutex_init(&db->mut, NULL);

    int i, j, k = 0;
    for (i = 0; i < SUBSCRIPTIONS; ++i) {
        db->sumPrices[i] = 0;
        db->totalSize[i] = 0;
        db->numTrades[i] = 0;
    }

    const char* const headers[METRICS] = {"Price,Size,Timestamp,TimeReceived,TimeSaved", "AveragePrice,TotalSize,Timestamp",
                             "BestIndicator,Coefficient,CorrelationTime,Timestamp"};

    const char* const filepaths[METRICS+1] = {"generated-files/", "generated-files/trade-data/", "generated-files/moving-averages/",
                               "generated-files/pearson-correlation-coefficients/"};

    struct stat st = {0};
    for (i = 0; i < METRICS+1; ++i) {
        if (stat(filepaths[i], &st) == -1) {
            mkdir(filepaths[i], 0777);
        }
    }
    for (i = 1; i < METRICS+1; ++i) {
        k = i-1;
        for (j = 0; j < SUBSCRIPTIONS; ++j) {
            char fileSpec[strlen(filepaths[i]) + strlen(subsArr[j]) + strlen(EXTENSION) + 1];
            snprintf(fileSpec, sizeof(fileSpec), "%s%s%s", filepaths[i], subsArr[j], EXTENSION);
            if (k == 1) {
                db->files[k][j] = fopen(fileSpec, "w+");
            }
            else {
                db->files[k][j] = fopen(fileSpec, "w");
            }
            fprintf(db->files[k][j], "%s\n", headers[k]);
        }
    }
}

static void databaseDelete(database *db) {
    for (int i = 0; i < METRICS; ++i) {
        for (int j = 0; j < SUBSCRIPTIONS; ++j) {
            fclose(db->files[i][j]);
        }
    }
    pthread_mutex_destroy(&db->mut);
}

static void connect_client(lws_sorted_usec_list_t *sul) {
    clientConnectData *ccDat = lws_container_of(sul, clientConnectData, sul);

    if (!lws_client_connect_via_info(&ccDat->ccinfo)) {
        lws_retry_sul_schedule(ccDat->ccinfo.context, 0, sul, &ccDat->retry, connect_client, &ccDat->retry_count);
    }
}

static void *producer (void *args) {
    struct lws_context *context = args;
    producerData *proDat = lws_context_user(context);

    while (!proDat->interrupted) {
        lws_service(context, 0);
    }
    return NULL;
}

static void *consumer (void *args) {
    consumerData *conDat = args;
    trade t;

    while (!conDat->consumerFlag) {
        pthread_mutex_lock(&conDat->fifo.mut);
        while (conDat->fifo.empty && !conDat->consumerFlag) {
            //printf("consumer: queue EMPTY.\n");
            pthread_cond_wait(&conDat->fifo.notEmpty, &conDat->fifo.mut);
        }
        if (conDat->fifo.empty) {
            pthread_mutex_unlock(&conDat->fifo.mut);
            return NULL;
        }
        queueDel(&conDat->fifo, &t);
        pthread_cond_signal(&conDat->fifo.notFull);
        pthread_mutex_unlock(&conDat->fifo.mut);

        pthread_mutex_lock(&conDat->DB.mut);
        conDat->DB.totalSize[t.subIndex] += t.size;
        conDat->DB.sumPrices[t.subIndex] += t.price*t.count;
        conDat->DB.numTrades[t.subIndex] += t.count;
        pthread_mutex_unlock(&conDat->DB.mut);

        fprintf(conDat->DB.files[0][t.subIndex], "%lf,%lf,%llu,%llu,%llu\n", t.price, t.size, t.timestamp, t.timeReceived, unixTimeInMs());
    }
    return NULL;
}

static void *everyInterval (void *args) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec += INTERVAL;

    intervalData *intDat = args;

    pearsonCorr pearson, maxPearson;

    double sumPrices[SUBSCRIPTIONS][INTERVALS], numTrades[SUBSCRIPTIONS][INTERVALS], totalSize[SUBSCRIPTIONS][INTERVALS],
           priceSum, sizeTotal, priceAvg[SUBSCRIPTIONS][DATAPOINTS], avgVec[DATAPOINTS];

    int i, j, k, trades, bestIndicator, intervalCount = 0, minuteCount = 0;

    char line[MAX_LINE+1];

    for (i = 0; i < SUBSCRIPTIONS; ++i) {
        for (j = 0; j < INTERVALS; ++j) {
            sumPrices[i][j] = 0;
            numTrades[i][j] = 0;
            totalSize[i][j] = 0;
        }
        for (j = 0; j < DATAPOINTS; ++j) {
            priceAvg[i][j] = NAN;
        }
    }
    for (i = 0; i < DATAPOINTS; ++i) {
        avgVec[i] = NAN;
    }

    while (!intDat->finish) {
        pthread_mutex_lock(&intDat->DB->mut);

        while (!intDat->finish && pthread_cond_timedwait(&intDat->finishUp, &intDat->DB->mut, &ts) != ETIMEDOUT);
        ts.tv_sec += INTERVAL;

        if (intDat->finish) {
            pthread_mutex_unlock(&intDat->DB->mut);
            return NULL;
        }
        // For every instrument, fetch the data of the latest interval
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            sumPrices[i][intervalCount] = intDat->DB->sumPrices[i];
            numTrades[i][intervalCount] = intDat->DB->numTrades[i];
            totalSize[i][intervalCount] = intDat->DB->totalSize[i];
            intDat->DB->sumPrices[i] = 0;
            intDat->DB->totalSize[i] = 0;
            intDat->DB->numTrades[i] = 0;
        }
        // No longer need the mutex, as the only shared resources are sumPrices, totalSize and numTrades, and we are done with those
        pthread_mutex_unlock(&intDat->DB->mut);

        // Calculation of simple moving average and total size for the latest <INTERVALS> window
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            priceSum = 0;
            trades = 0;
            sizeTotal = 0;
            for (j = 0; j < INTERVALS; ++j) {
                priceSum += sumPrices[i][j];
                trades += numTrades[i][j];
                sizeTotal += totalSize[i][j];
            }
            if (minuteCount < DATAPOINTS) {
                if (trades != 0) {
                    priceAvg[i][minuteCount] = priceSum / trades;
                }
                else {
                    priceAvg[i][minuteCount] = NAN;
                }
                fseek(intDat->DB->files[1][i], 0, SEEK_END);
                fprintf(intDat->DB->files[1][i], "%lf,%lf,%llu\n", priceAvg[i][minuteCount], sizeTotal, unixTimeInMs());
            }
            else {
                memmove(&priceAvg[i][0], &priceAvg[i][1], (DATAPOINTS-1) * sizeof(priceAvg[i][0]));
                if (trades != 0) {
                    priceAvg[i][DATAPOINTS-1] = priceSum / trades;
                }
                else {
                    priceAvg[i][DATAPOINTS-1] = NAN;
                }
                fseek(intDat->DB->files[1][i], 0, SEEK_END);
                fprintf(intDat->DB->files[1][i], "%lf,%lf,%llu\n", priceAvg[i][DATAPOINTS-1], sizeTotal, unixTimeInMs());
            }
        }
        ++minuteCount;

        // Calculation of Pearson Correlation Coefficient: for each instrument, its coefficient with every other instrument.
        for (i = 0; i < SUBSCRIPTIONS; ++i) {
            bestIndicator = -1;
            maxPearson.coeff = 0;
            maxPearson.corrTime = -1;
            for (j = 0; j < SUBSCRIPTIONS; ++j) {
                fseek(intDat->DB->files[1][j], 0, SEEK_SET);
                fgets(line, MAX_LINE, intDat->DB->files[1][j]);

                for (k = 0; k < DATAPOINTS && fgets(line, MAX_LINE, intDat->DB->files[1][j]) != NULL; ++k) {
                    sscanf(line,"%lf,%*f,%llu\n", &avgVec[k], &pearson.corrTime);
                }

                pearson.coeff = pearsonCorrCoeff(DATAPOINTS, priceAvg[i], avgVec);
                
                if (!isnan(pearson.coeff) && fabs(maxPearson.coeff) < fabs(pearson.coeff)) {
                    bestIndicator = j;
                    maxPearson.coeff = pearson.coeff;
                    maxPearson.corrTime = pearson.corrTime;
                }

                for ( ; k < minuteCount-DATAPOINTS; ++k) {
                    fgets(line, MAX_LINE, intDat->DB->files[1][j]);
                    memmove(&avgVec[0], &avgVec[1], (DATAPOINTS-1) * sizeof(avgVec[0]));
                    sscanf(line,"%lf,%*f,%llu\n", &avgVec[DATAPOINTS-1], &pearson.corrTime);

                    pearson.coeff = pearsonCorrCoeff(DATAPOINTS, priceAvg[i], avgVec);
                    
                    if (!isnan(pearson.coeff) && fabs(maxPearson.coeff) < fabs(pearson.coeff)) {
                        bestIndicator = j;
                        maxPearson.coeff = pearson.coeff;
                        maxPearson.corrTime = pearson.corrTime;
                    }
                }
                if (i != j) {
                    for ( ; k < minuteCount; ++k) {
                        fgets(line, MAX_LINE, intDat->DB->files[1][j]);
                        memmove(&avgVec[0], &avgVec[1], (DATAPOINTS-1) * sizeof(avgVec[0]));
                        sscanf(line,"%lf,%*f,%llu\n", &avgVec[DATAPOINTS-1], &pearson.corrTime);

                        pearson.coeff = pearsonCorrCoeff(DATAPOINTS, priceAvg[i], avgVec);
                        
                        if (!isnan(pearson.coeff) && fabs(maxPearson.coeff) < fabs(pearson.coeff)) {
                            bestIndicator = j;
                            maxPearson.coeff = pearson.coeff;
                            maxPearson.corrTime = pearson.corrTime;
                        }
                    }
                }
            }

            // Save coefficients to their respective files
            if (bestIndicator != -1) {
                fprintf(intDat->DB->files[2][i], "%s,%lf,%llu,%llu\n", (*intDat->subs)[bestIndicator],
                                                                        maxPearson.coeff,
                                                                        maxPearson.corrTime,
                                                                        unixTimeInMs());
            }
            else {
                fprintf(intDat->DB->files[2][i], "%lf,%lf,%lf,%llu\n", NAN,
                                                                       NAN,
                                                                       NAN,
                                                                       unixTimeInMs());
            }
        }
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
    pthread_mutex_destroy(&q->mut);
    pthread_cond_destroy(&q->notFull);
    pthread_cond_destroy(&q->notEmpty);
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
