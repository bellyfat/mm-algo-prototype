import aiohttp
import api_auth
from typing import List, Union
from collections import OrderedDict
import asyncio


class Gateway:
    _bybit_auth: api_auth.BybitApiAuth
    _binance_auth: api_auth.BinanceApiAuth
    is_rate_limited = False

    def __init__(self, api_pth_bybit: str, api_pth_binance: str) -> None:
        self._bybit_auth = api_auth.BybitApiAuth(file_path=api_pth_bybit)
        self._binance_auth = api_auth.BinanceApiAuth(file_path=api_pth_binance)

    def prepare_bybit_new_order(self, order: OrderedDict,
                                is_queued: List[bool],
                                ord_link_id: List[Union[str, None]]) -> None:
        is_queued[0] = True
        order_bdy_str = self._bybit_auth.get_order_auth_body(order=order)
        asyncio.create_task(
            coro=self.send_bybit_new_order(order=order_bdy_str,
                                           is_queued=is_queued,
                                           ord_link_id=ord_link_id))

    def prepare_binance_new_order(self, order: OrderedDict) -> None:
        order_bdy_str = self._binance_auth.get_order_auth_body(order=order)
        asyncio.create_task(
            coro=self.send_binance_new_order(order=order_bdy_str))

    def prepare_bybit_amend_order(self, order: OrderedDict,
                                  is_queued: List[bool]) -> None:
        is_queued[0] = True
        order_bdy_str = self._bybit_auth.get_order_auth_body(order=order)
        asyncio.create_task(
            coro=self.amend_bybit_order(order=order_bdy_str,
                                        is_queued=is_queued))

    async def check_bybit_rate_limits(self, res_bdy: dict) -> None:
        if (res_bdy.get('rate_limit_status') == 0
                and not self.is_rate_limited):
            sleep_for = (res_bdy.get('rate_limit_reset_ms')
                         - api_auth.get_milli_timestamp()) / 1000.0
            if sleep_for > 0:
                self.is_rate_limited = True
                print(sleep_for)
                await asyncio.sleep(delay=sleep_for)
                self.is_rate_limited = False

    async def send_bybit_new_order(self, order: str,
                                   is_queued: List[bool],
                                   ord_link_id: List[Union[str, None]]) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url='https://api.bybit.com/v2/private/order/create',
                    data=order, headers={'Content-Type': 'application/json'},
                    ssl=True) as res:
                try:
                    res_bdy = await res.json()
                    if res_bdy.get('ret_code') != 0:
                        ord_link_id[0] = None
                    await self.check_bybit_rate_limits(res_bdy=res_bdy)
                except aiohttp.ContentTypeError as e:
                    print(e)
                finally:
                    is_queued[0] = False

    async def send_binance_new_order(self, order: str) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url='https://dapi.binance.com/dapi/v1/order', data=order,
                    headers=self._binance_auth.headers, ssl=True) as res:
                try:
                    resp = await res.json()
                    print('Binance Response Status:', res.status)
                except aiohttp.ContentTypeError as e:
                    print(e)

    async def amend_bybit_order(self, order: str,
                                is_queued: List[bool]) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url='https://api.bybit.com/v2/private/order/replace',
                    data=order, headers={'Content-Type': 'application/json'},
                    ssl=True) as res:
                try:
                    res_bdy = await res.json()
                    await self.check_bybit_rate_limits(res_bdy=res_bdy)
                except aiohttp.ContentTypeError as e:
                    print(e)
                finally:
                    is_queued[0] = False

