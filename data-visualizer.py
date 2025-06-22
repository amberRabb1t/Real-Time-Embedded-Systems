import matplotlib.pyplot as plt
import numpy as np
import csv
import os
from collections import Counter

subscriptions = ["BTC-USDT", "ADA-USDT", "ETH-USDT", "DOGE-USDT", "XRP-USDT", "SOL-USDT", "LTC-USDT", "BNB-USDT"]
extension = ".txt"
print("\n")

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
            pcc_data_coeff.append(float(row[1]))
            pcc_data_time.append((int(row[3]) - int(row[2])) / (1000*60))
            pcc_data_timestamp.append(int(row[3]))

    fig, ax = plt.subplots(2, 2)
    ax[0, 0].plot(range(len(trade_data_timestamp)), np.subtract(trade_data_timereceived, trade_data_timestamp), 'r-', linewidth=1)
    ax[0, 0].set_xlabel('Data Entry #', fontsize=12)
    ax[0, 0].set_ylabel('Milliseconds', color='r', fontsize=14)
    ax[0, 0].set_title('OKX send delay (Milliseconds)', fontsize=16)  
    ax[0, 0].grid(True)

    ax[0, 1].plot(range(len(trade_data_timereceived)), np.subtract(trade_data_timesaved, trade_data_timereceived), 'b-', linewidth=1)
    ax[0, 1].set_xlabel('Data Entry #', fontsize=12)
    ax[0, 1].set_ylabel('Milliseconds', color='b', fontsize=14)
    ax[0, 1].set_title('Program save delay (Milliseconds)', fontsize=16)  
    ax[0, 1].grid(True)

    ax[1, 0].plot(range(len(ma_data_timestamp) - 1), np.diff(ma_data_timestamp), 'r-')
    ax[1, 0].set_xlabel('Data Entry #', fontsize=12)
    ax[1, 0].set_ylabel('Milliseconds', color='r', fontsize=14)
    ax[1, 0].set_title('Moving Average time delay (Milliseconds)', fontsize=16)
    ax[1, 0].grid(True)

    ax[1, 1].plot(range(len(pcc_data_timestamp) - 1), np.diff(pcc_data_timestamp), 'bo', markersize=2)
    ax[1, 1].set_xlabel('Data Entry #', fontsize=12)
    ax[1, 1].set_ylabel('Milliseconds', color='b', fontsize=14)
    ax[1, 1].set_title('Pearson Correlation Coefficient time delay (Milliseconds)', fontsize=16)
    ax[1, 1].grid(True)
    plt.suptitle(f'Time Delays for {label}', fontsize=18)


    fig, ax = plt.subplots(2, 2)
    ax[0, 0].plot(range(len(ma_data_avg)), ma_data_avg, 'r-')
    ax[0, 0].set_xlabel('Data Entry #', fontsize=12)
    ax[0, 0].set_ylabel('Average Price', color='r', fontsize=14)
    ax[0, 0].set_title(f'Moving Average and Total Size', fontsize=16)
    ax[0 ,0].grid(True)

    ax2 = ax[0, 0].twinx()
    ax2.plot(range(len(ma_data_size)), ma_data_size, 'b-', linewidth=0.75)
    ax2.set_ylabel('Total Size', color='b', fontsize=14)

    ax[0, 1].plot(range(len(pcc_data_coeff)), pcc_data_coeff, 'ro', markersize=2)
    ax[0, 1].set_xlabel('Data Entry #', fontsize=12)
    ax[0, 1].set_ylabel('Coefficient', color='r', fontsize=14)
    ax[0, 1].set_title('Largest Pearson Correlation Coefficient', fontsize=16)
    ax[0, 1].grid(True)

    ax[1, 0].plot(range(len(pcc_data_time)), pcc_data_time, 'bo', markersize=2)
    ax[1, 0].set_xlabel('Data Entry #', fontsize=12)
    ax[1, 0].set_ylabel('Minutes Ago', color='b', fontsize=14)
    ax[1, 0].set_title('Correlation Time of Largest Coefficient (Minutes Ago)', fontsize=16)
    ax[1, 0].grid(True)

    dummy = np.convolve(pcc_data_time, np.ones(15)/15, mode='valid')
    ax[1, 1].plot(range(len(dummy)), dummy, 'r-', linewidth=1)
    ax[1, 1].set_xlabel('Data Entry #', fontsize=12)
    ax[1, 1].set_ylabel('Sliding Window Size = 15', color='r', fontsize=14)
    ax[1, 1].set_title('Moving Average of Correlation Time of Largest Coefficient (Minutes Ago)', fontsize=16)
    ax[1, 1].grid(True)

    ax2 = ax[1, 1].twinx()
    dummy = np.convolve(pcc_data_time, np.ones(150)/150, mode='valid')
    ax2.plot(range(len(dummy)), dummy, 'b-')
    ax2.set_ylabel('Sliding Window Size = 150', color='b', fontsize=14)
    plt.suptitle(f'Moving Average, Size and Pearson Correlation Coefficient data for {label}', fontsize=18)

    print(f'Predictor rankings for {label}: {Counter(pcc_data_label)}')
    print(f'Correlation Time standard deviation: {np.std(pcc_data_time)}')

plt.show()
print("\n")
