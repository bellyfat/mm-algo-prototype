import aiohttp
import multiprocessing as mp
from typing import Dict
import api_auth
import urllib.parse
from collections import OrderedDict
import json
import asyncio


class Gateway:
    _bybit_auth: api_auth.BybitApiAuth
    _binance_auth: api_auth.BinanceApiAuth
    _strategy_conns: Dict[str, mp.connection.Connection]

    def __init__(self, api_pth_bybit: str, api_pth_binance: str,
                 strategy_conns: Dict[str, mp.connection.Connection]) -> None:
        self._bybit_auth = api_auth.BybitApiAuth(file_path=api_pth_bybit)
        self._binance_auth = api_auth.BinanceApiAuth(file_path=api_pth_binance)
        self._strategy_conns = strategy_conns

    def run_gateway(self) -> None:
        for key, conn in self._strategy_conns.items():
            if conn.poll():
                data: OrderedDict = conn.recv()
                if key == 'bybit-new-order':
                    order_bdy_str = self._bybit_auth.get_order_auth_body(
                        order=data)
                    asyncio.get_event_loop().run_until_complete(
                        future=self.send_bybit_new_order(order=order_bdy_str))
                elif key == 'binance-new-order':
                    order_bdy_str = self._binance_auth.get_order_auth_body(
                        order=data)
                    asyncio.get_event_loop().run_until_complete(
                        future=self.send_binance_new_order(order=order_bdy_str))
                elif key == 'bybit-amend':
                    order_bdy_str = self._bybit_auth.get_order_auth_body(
                        order=data)
                    asyncio.get_event_loop().run_until_complete(
                        future=self.amend_bybit_order(order=order_bdy_str))

    @staticmethod
    async def send_bybit_new_order(order: str) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url='https://api.bybit.com/v2/private/order/create',
                    data=order, headers={'Content-Type': 'application/json'},
                    ssl=True) as res:
                await res.json()

    async def send_binance_new_order(self, order: str) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url='https://dapi.binance.com/dapi/v1/order', data=order,
                    headers=self._binance_auth.headers, ssl=True) as res:
                await res.json()

    async def amend_bybit_order(self, order: str) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url='https://api.bybit.com/v2/private/order/replace',
                    data=order, headers={'Content-Type': 'application/json'},
                    ssl=True) as res:
                res_bdy = await res.json()
                if res_bdy.get('rate_limit_status') == 0:
                    print('RATE LIMIT STOP!!!!!')

