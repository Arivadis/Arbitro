# -*- coding: utf-8 -*-

import ccxt.pro
from asyncio import gather, run, sleep
import aiosqlite
import datetime

orderbooks = {}


async def handle_all_orderbooks(orderbooks):
    print('We have the following orderbooks:')
    async with aiosqlite.connect('orderbook.db') as conn:
        async with conn.cursor() as c:
            await c.execute('''CREATE TABLE IF NOT EXISTS orderbooks 
                                (timestamp TEXT, id TEXT, symbol TEXT, asks_price REAL, 
                                asks_volume REAL, bids_price REAL, bids_volume REAL)''')
            await conn.commit()
            for id, orderbooks_by_symbol in orderbooks.items():
                for symbol in orderbooks_by_symbol.keys():
                    orderbook = orderbooks_by_symbol[symbol]
                    timestamp = datetime.datetime.now()
                    asks_price, asks_volume = orderbook['asks'][0]
                    bids_price, bids_volume = orderbook['bids'][0]
                    print(timestamp, id, symbol, asks_price, asks_volume, bids_price, bids_volume)
                    # Insert into SQLite
                    await c.execute("INSERT INTO orderbooks VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (timestamp, id, symbol, asks_price, asks_volume, bids_price, bids_volume))
            await conn.commit()


async def symbol_loop(exchange, id, symbol):
    print('Starting', id, symbol)
    while True:
        try:
            orderbook = await exchange.watch_order_book(symbol)
            orderbooks[id] = orderbooks.get(id, {})
            orderbooks[id][symbol] = orderbook
            print('===========================================================')
            await handle_all_orderbooks(orderbooks)
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
    conn = await aiosqlite.connect('orderbook.db')
    print("Connected to database.")

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
    try:
        loops = [exchange_loop(id, config) for id, config in configs.items()]
        await gather(*loops)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        await conn.close()
        print("Database connection closed.")

async def shutdown():
    print("Shutting down...")
    # Add any cleanup operations here

# Run the main function and handle graceful shutdown
try:
    run(main())
except KeyboardInterrupt:
    pass
finally:
    run(shutdown())
