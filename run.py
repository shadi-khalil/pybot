import asyncio
import os
# import sys
import pandas as pd
import time
from datetime import timedelta, datetime, timezone
# import subprocess, platform
from binance.client import Client
import os.path
from numerize import numerize
from dateutil.relativedelta import relativedelta as rd
import requests
from colorama import init
from termcolor import colored

intervals = ['days', 'hours', 'minutes', 'seconds']
TEL_TOKEN = '1401074069:AAHw6A-UAuez4ZSE2LB5hWfoTt0fojK7YnU'
TEL_CHAT = '-469633022'
client = Client(api_key='30mxrs4WUanqOCPpcqJYshlNmTJjDd6usAjeUjQOK1QEBHAhwygCiUFtMLtU9qcp',
                api_secret='z3p8WgubQhIGW1sG2E23P2KVp7dedbT9hZcFNwdQhx7V0rvrr54o3KWCn6ul0kO4')


async def savetrades(symbol):
    save_file = f'{symbol}_trades.csv'
    open_orders_id = []
    open_orders, open_sell, open_buy = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    req_offset = None

    while True:
        print("\033c", end="")
        try:
            trades = client.get_recent_trades(symbol=symbol)
            time.sleep(0.5)
            all_my_orders = client.get_all_orders(symbol=symbol)
            orders_df = pd.DataFrame(all_my_orders) if all_my_orders != [
            ] else pd.DataFrame()
            if not orders_df.empty:
                orders_df = orders_df.set_index('orderId')
                open_orders = orders_df[
                    (orders_df['status'] == 'PARTIALLY_FILLED') | (orders_df['status'] == 'NEW')].copy()
                open_orders['price'] = open_orders['price'].astype(float)
                open_sell = open_orders[open_orders['side'] == 'SELL'].copy()
                open_buy = open_orders[open_orders['side'] == 'BUY'].copy()
                open_sell.sort_values(
                    by=['price'], ascending=True, inplace=True)
                open_buy.sort_values(
                    by=['price'], ascending=False, inplace=True)

            if open_orders_id and len(open_orders.index) > 0:
                for i in open_orders_id:
                    # print('open_orders_id')
                    if i in orders_df.index:
                        # print('open_orders_id is inside orders_df.index')
                        order = orders_df.loc[i]
                        if order['status'] == 'FILLED':
                            # print('status FILLED')
                            period = rd(
                                seconds=(int(order['updateTime'] / 1000)) - (int(order['time'] / 1000)))
                            msg = f"<b>#{order['symbol']}</b>  <b>{order['side']}</b> {(order['executedQty'])} is filled at price {order['price']}in \n {' '.join('{} {}'.format(getattr(period, k), k) for k in intervals if getattr(period, k))}"
                            payload = {
                                'chat_id': TEL_CHAT,
                                'text': msg,
                                'parse_mode': 'HTML'
                            }

                            req = requests.post(
                                f"https://api.telegram.org/bot{TEL_TOKEN}/sendMessage", data=payload)
                            if req.status_code == 200:
                                print('-' * 40)
                                print("SENT ALERT", msg)
                                print('-' * 40)

            open_orders_id = not open_orders.empty and open_orders.index.to_list()
            # if open_orders_id:
            # print(open_orders_id)

            df = pd.DataFrame(trades, dtype=float)
            if not df.empty:
                df.drop(['quoteQty', 'isBestMatch'], axis=1, inplace=True)

            if os.path.isfile(save_file):
                last_saved_trade = pd.read_csv(save_file).iloc[-1]['id']
                dfn = df[df['id'] > last_saved_trade]

                if len(dfn.index) > 0:
                    dfn.to_csv(save_file, mode='a', header=False, index=False)
                    print(f"saved {len(dfn.index)} new trades")

            else:
                df.to_csv(save_file, header=True, index=False)
                print(f"saved {len(df.index)} new trades")

        except Exception as e:
            print(type(e).__name__, str(e), str(e.args))
            # raise e
        trades_stats = pd.read_csv(save_file)
        seller5 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 1.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 5 * 60) * 1000)]['qty'].sum())
        seller60 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 1.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 60 * 60) * 1000)]['qty'].sum())
        buyer5 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 0.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 5 * 60) * 1000)]['qty'].sum())
        buyer60 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 0.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 60 * 60) * 1000)]['qty'].sum())
        since_buy_order = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 1.0) & (
            trades_stats['time'] > open_buy.iloc[0]['time']) & (trades_stats['price'] == open_buy.iloc[0][
                'price'])]['qty'].sum()) if not open_buy.empty else None
        since_sell_order = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 0.0) & (
            trades_stats['time'] > open_sell.iloc[0]['time']) & (trades_stats['price'] == open_sell.iloc[0][
                'price'])]['qty'].sum()) if not open_sell.empty else None

        seller1 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 1.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 1 * 60) * 1000)]['qty'].sum())
        seller240 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 1.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 240 * 60) * 1000)]['qty'].sum())
        buyer1 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 0.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 1 * 60) * 1000)]['qty'].sum())
        buyer240 = numerize.numerize(trades_stats[(trades_stats['isBuyerMaker'] == 0.0) & (trades_stats['time'] > (
            datetime.utcnow().replace(tzinfo=timezone.utc).timestamp() - 240 * 60) * 1000)]['qty'].sum())

        print('#' * 60)
        print(f'###### {symbol} ######')
        print('#' * 60)
        print('## SELLER ##')
        print(colored(
            f"1min: {seller1}    5min: {seller5}    1h: {seller60}      4h: {seller240}      {'buy order:' + str(since_buy_order) if since_buy_order is not None else ''}",
            'red'))
        print('')
        print('## BUYER ##')
        print(colored(
            f"1min: {buyer1}     5min: {buyer5}     1h: {buyer60}      4h: {buyer240}      {'sell order:' + str(since_sell_order) if since_sell_order is not None else ''}",
            'green'))
        print('#' * 60)
        req = requests.get(
            f"https://api.telegram.org/bot{TEL_TOKEN}/getUpdates?{'offset=' + str(req_offset) if req_offset is not None else ''}")
        if req.status_code == 200:
            res = req.json()
            if len(res['result']) > 0:
                req_offset = res['result'][-1]['update_id'] + 1
                if res['result'][-1]['message']['text'] == '/status':
                    msg = f"#SELLER# 5min: {seller5}    1h: {seller60}      {'sold since buy order:' + str(since_buy_order) if since_buy_order is not None else ''} \n \n =======================\n \n #BUYER#  5min: {buyer5}     1h: {buyer60}      {'bought since sell order:' + str(since_sell_order) if since_sell_order is not None else ''}"
                    payload = {
                        'chat_id': TEL_CHAT,
                        'text': msg,
                        'parse_mode': 'HTML'
                    }

                    req = requests.post(
                        f"https://api.telegram.org/bot{TEL_TOKEN}/sendMessage", data=payload)
        time.sleep(1)


if __name__ == '__main__':
    save_trades = asyncio.get_event_loop()

    try:
        asyncio.ensure_future(savetrades('REEFBTC'))
        save_trades.run_forever()

    except KeyboardInterrupt:
        pass
    finally:
        save_trades.close()
