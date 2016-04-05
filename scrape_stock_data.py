from __future__ import print_function
import requests
import json
import sqlite3
import time
import pandas as pd


def get_stock_data(stock):
    request = "http://chartapi.finance.yahoo.com/instrument/1.0/{}/chartdata;type=quote;range=1d/json".format(stock)
    return json.loads(requests.get(request).text[30:-1])


def read_stock_data(symbols, filename='stocks.db'):
    print("Preparing database... ", end="")
    # Open sqlite database connection and create table if it does not exist
    conn = sqlite3.connect(filename)
    conn.execute(''' CREATE TABLE IF NOT EXISTS stocks 
                    (timestamp INTEGER, symbol TEXT, 
                     close REAL, high REAL, low REAL,
                     open REAL, volume INTEGER,
                    UNIQUE (timestamp, symbol) ON CONFLICT REPLACE);''')
    conn.execute("CREATE INDEX IF NOT EXISTS Idx1 ON stocks(timestamp);")
    conn.execute("CREATE INDEX IF NOT EXISTS Idx2 ON stocks(symbol);")
    print("DONE")


    successful = 0
    failed = []
    for symbol in symbols:
        print("Parsing {}... ".format(symbol), end="")
        try:
            stock_data = get_stock_data(symbol)

            for datum in stock_data['series']:
                conn.execute('''INSERT INTO stocks (timestamp, symbol, close, high, low, open, volume) 
                                            VALUES 
                                ({timestamp}, "{symbol}", {close}, {high}, {low}, {open}, {volume}) '''
                             .format(timestamp=int(datum['Timestamp']), 
                                     symbol=symbol, 
                                     close=float(datum['close']),
                                     high=float(datum['high']), 
                                     low=float(datum['low']), 
                                     open=float(datum['open']),
                                     volume=int(datum['volume'])))
            conn.commit()
            successful += 1
            print("DONE")
        except (ValueError, KeyError) as e:
            failed.append(symbol)
            print(e)

    conn.close()

    print("Successfully parsed {} symbols".format(successful))
    print("Failed to parse the following symbols: {}".format(failed))


def update_symbol_list():
    print("Updating symbol list... ", end="")
    from ftplib import FTP
    ftp = FTP('ftp.nasdaqtrader.com')     # connect to host, default port
    ftp.login()
    ftp.cwd("SymbolDirectory")
    ftp.retrbinary('RETR nasdaqlisted.txt', open('nasdaqlisted.txt', 'wb').write)
    ftp.retrbinary('RETR otherlisted.txt', open('otherlisted.txt', 'wb').write)
    ftp.quit()
    print("DONE")
        

if __name__ == "__main__":
    update_symbol_list()

    nasdaq_symbols = pd.read_csv('nasdaqlisted.txt', delimiter='|')["Symbol"]
    read_stock_data(nasdaq_symbols)

    other_symbols = pd.read_csv('otherlisted.txt', delimiter='|')["ACT Symbol"]
    read_stock_data(other_symbols)
