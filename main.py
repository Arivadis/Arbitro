# -*- coding: utf-8 -*-

import ccxt.pro
from asyncio import gather, run
import sqlite3
import datetime

orderbooks = {}


def handle_all_orderbooks(orderbooks):
    print('We have the following orderbooks:')
    for id, orderbooks_by_symbol in orderbooks.items():
        for symbol in orderbooks_by_symbol.keys():
            orderbook = orderbooks_by_symbol[symbol]
            timestamp = datetime.datetime.now()
            asks_price, asks_volume = orderbook['asks'][0]
            bids_price, bids_volume = orderbook['bids'][0]
            print(timestamp, id, symbol, asks_price, asks_volume, bids_price, bids_volume)
            # Insert into SQLite
            insert_into_db(timestamp, id, symbol, asks_price, asks_volume, bids_price, bids_volume)

def insert_into_db(timestamp, id, symbol, asks_price, asks_volume, bids_price, bids_volume):
    conn = sqlite3.connect('orderbook.db')
    c = conn.cursor()
    # Create table if not exists
    c.execute('''CREATE TABLE IF NOT EXISTS orderbooks 
                 (timestamp TEXT, id TEXT, symbol TEXT, asks_price REAL, asks_volume REAL, bids_price REAL, bids_volume REAL)''')
    # Insert data
    c.execute("INSERT INTO orderbooks VALUES (?, ?, ?, ?, ?, ?, ?)",
              (timestamp, id, symbol, asks_price, asks_volume, bids_price, bids_volume))
    conn.commit()
    conn.close()
async def symbol_loop(exchange, id, symbol):
    print('Starting', id, symbol)
    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol)
            orderbooks[id] = orderbooks.get(id, {})
            orderbooks[id][symbol] = orderbook
            print('===========================================================')
            #
            # here you can do what you want
            # with the most recent versions of each orderbook you have so far
            #
            # you can also wait until all of them are available
            # by just looking into all the orderbooks and counting them
            #
            # we just print them here to keep this example simple
            #
            handle_all_orderbooks(orderbooks)
        except Exception as e:
            print(str(e))
            break  # you can break just this one loop if it fails


async def exchange_loop(id, config):
    print('Starting', id)
    exchange = getattr(ccxt.pro, config['id'])({
        'options': config['options'],
    })
    loops = [symbol_loop(exchange, id, symbol) for symbol in config['symbols']]
    await gather(*loops)
    await exchange.close()


async def main():
    conn = sqlite3.connect('orderbook.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS orderbooks 
                 (timestamp TEXT, id TEXT, symbol TEXT, asks_price REAL, asks_volume REAL, bids_price REAL, bids_volume REAL)''')
    conn.commit()
    conn.close()
    configs = {
        'Binance spot': {
            'id': 'binance',
            'symbols': ['STMX/USDT', 'ANKR/USDT'],
            'options': {
                'defaultType': 'spot',
            },
        },
        'Binance futures': {
            'id': 'binance',
            'symbols': ['STMX/USDT', 'ANKR/USDT'],
            'options': {
                'defaultType': 'future',
            },
        },
    }
    loops = [exchange_loop(id, config) for id, config in configs.items()]
    await gather(*loops)


run(main())