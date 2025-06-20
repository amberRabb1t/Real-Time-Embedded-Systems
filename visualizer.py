import matplotlib.pyplot as plt
import numpy as np
import csv
import os

subscriptions = ["BTC-USDT", "ADA-USDT", "ETH-USDT", "DOGE-USDT", "XRP-USDT", "SOL-USDT", "LTC-USDT", "BNB-USDT"]
extension = ".txt"

for label in subscriptions:
    trade_data_timestamp = []
    trade_data_timereceived = []
    trade_data_timesaved = []

    ma_data_avg = []
    ma_data_size = []
    ma_data_timestamp = []

    pcc_data_label = []
    pcc_data_coeff = []
    pcc_data_time = []
    pcc_data_timestamp = []

    trades_path = os.path.join('generated-files', 'trade-data', label + extension)
    ma_path = os.path.join('generated-files', 'moving-averages', label + extension)
    pcc_path = os.path.join('generated-files', 'pearson-correlation-coefficients', label + extension)

    # Read and convert Trade data
    with open(trades_path, 'r') as trades_file:
        trades_reader = csv.reader(trades_file)
        next(trades_reader, None) # Skip header

        for row in trades_reader:
            trade_data_timestamp.append(int(row[2]))
            trade_data_timereceived.append(int(row[3]))
            trade_data_timesaved.append(int(row[4]))

    # Read and convert Moving Average data
    with open(ma_path, 'r') as ma_file:
        ma_reader = csv.reader(ma_file)
        next(ma_reader, None) # Skip header
        firstRow = next(ma_reader, None) # Get first line of data
        
        ma_data_avg.append(float(firstRow[0]))
        ma_data_size.append(float(firstRow[1]))
        ma_data_timestamp.append(int(firstRow[2]))

        for row in ma_reader:
            ma_data_avg.append(float(row[0]))
            ma_data_size.append(float(row[1]))
            ma_data_timestamp.append(int(row[2]))

    # Read and convert Pearson Correlation Coefficient data
    with open(pcc_path, 'r') as pcc_file:
        pcc_reader = csv.reader(pcc_file)
        next(pcc_reader, None) # Skip header
        firstRow = next(pcc_reader, None)

        pcc_data_timestamp.append(int(firstRow[3]))

        for row in pcc_reader:
            pcc_data_label.append(row[0])
            pcc_data_coeff.append(abs(float(row[1])))
            pcc_data_time.append((int(row[3]) - int(row[2])) / (1000*60))
            pcc_data_timestamp.append(int(row[3]))

    fig, ax = plt.subplots(2, 2)
    ax[0, 0].plot(range(len(trade_data_timestamp)), np.subtract(trade_data_timereceived, trade_data_timestamp), 'r-')
    ax[0, 0].set_xlabel('Data Entry #')
    ax[0, 0].set_ylabel('Milliseconds', color='r')
    ax[0, 0].set_title('OKX send delay (Milliseconds)')
    ax[0, 0].grid(True)

    ax[0, 1].plot(range(len(trade_data_timereceived)), np.subtract(trade_data_timesaved, trade_data_timereceived), 'b-')
    ax[0, 1].set_xlabel('Data Entry #')
    ax[0, 1].set_ylabel('Milliseconds', color='b')
    ax[0, 1].set_title('Program save delay (Milliseconds)')
    ax[0, 1].grid(True)

    ax[1, 0].plot(range(len(ma_data_timestamp) - 1), np.diff(ma_data_timestamp), 'r-')
    ax[1, 0].set_xlabel('Data Entry #')
    ax[1, 0].set_ylabel('Milliseconds', color='r')
    ax[1, 0].set_title('Moving Average time delay (Milliseconds)')
    ax[1, 0].grid(True)

    ax[1, 1].plot(range(len(pcc_data_timestamp) - 1), np.diff(pcc_data_timestamp), 'b-')
    ax[1, 1].set_xlabel('Data Entry #')
    ax[1, 1].set_ylabel('Milliseconds', color='b')
    ax[1, 1].set_title('Pearson Correlation Coefficient time delay (Milliseconds)')
    ax[1, 1].grid(True)
    plt.suptitle(f'Time Delays for {label}', fontsize=16)


    fig, ax = plt.subplots(2, 2)
    ax[0, 0].plot(range(len(ma_data_avg)), ma_data_avg, 'r-')
    ax[0, 0].set_xlabel('Data Entry #')
    ax[0, 0].set_ylabel('Average Price', color='r')
    ax[0, 0].set_title(f'Moving Average')
    ax[0 ,0].grid(True)

    ax[0, 1].plot(range(len(ma_data_size)), ma_data_size, 'b-')
    ax[0, 1].set_xlabel('Data Entry #')
    ax[0, 1].set_ylabel('Size', color='b')
    ax[0, 1].set_title(f'Total Size')
    ax[0, 1].grid(True)

    ax[1, 0].plot(range(len(pcc_data_coeff)), pcc_data_coeff, 'r-')
    ax[1, 0].set_xlabel('Data Entry #')
    ax[1, 0].set_ylabel('Coefficient', color='r')
    ax[1, 0].set_title('Largest Pearson Correlation Coefficient')
    ax[1, 0].grid(True)

    ax[1, 1].plot(range(len(pcc_data_time)), pcc_data_time, 'b-')
    ax[1, 1].set_xlabel('Data Entry #')
    ax[1, 1].set_ylabel('Minutes Ago', color='b')
    ax[1, 1].set_title('Correlation Time of Largest Coefficient (Minutes Ago)')
    ax[1, 1].grid(True)
    plt.suptitle(f'Moving Average, Size and Pearson Correlation Coefficient data for {label}', fontsize=16)

plt.show()
