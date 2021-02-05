import asyncio
import ssl
import certifi
from abc import abstractmethod
import websockets
import json
import aiohttp
from api_auth import BybitApiAuth, BinanceApiAuth
import feed


class WsClient:
    _ssl_context: ssl.SSLContext
    _sub_message: str
    _feed: feed.Feed

    def __init__(self, sub_message: str, feed_object: feed.Feed) -> None:
        self._ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
        self._ssl_context.load_verify_locations(cafile=certifi.where())
        self._sub_message = sub_message
        self._feed = feed_object

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def on_connect(self,
                         websocket: websockets.WebSocketClientProtocol) -> None:
        pass

    def on_disconnect(self) -> None:
        pass

    async def connect(self, uri: str, **kwargs) -> None:
        try:
            websocket = await websockets.connect(
                uri=uri, ssl=self._ssl_context, **kwargs)
            try:
                await websocket.send(message=self._sub_message)
                await self.on_connect(websocket=websocket)
            except websockets.ConnectionClosed as e:
                print(e)
                await self.start()
        except websockets.InvalidHandshake as e:
            print(e)
            await self.start()

    async def http_get(self, uri: str, **kwargs) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=uri, ssl=self._ssl_context,
                                   **kwargs) as res:
                return await res.json()

    async def http_post(self, uri: str, data: str, **kwargs) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=uri, data=data, ssl=self._ssl_context,
                                    **kwargs) as res:
                return await res.json()


class BinanceWsClient(WsClient):
    _BASE_API_ENDPOINT = 'https://dapi.binance.com'
    _api_auth: BinanceApiAuth
    _depth_snapshot_path = '/dapi/v1/depth?symbol=BTCUSD_PERP&limit=1000'

    def __init__(self, api_file_path: str,
                 feed_object: feed.BinanceFeed) -> None:
        self._api_auth = BinanceApiAuth(file_path=api_file_path)
        sub_message = json.dumps(
            obj={'method': 'SUBSCRIBE', 'params': ['btcusd_perp@depth@100ms']})
        super().__init__(sub_message=sub_message, feed_object=feed_object)

    async def start(self) -> None:
        await self.connect(uri='wss://dstream.binance.com/ws/')

    def on_disconnect(self) -> None:
        self._feed.on_book_reset()

    async def get_depth_snapshot(self) -> None:
        res = await self.http_get(
            uri=self._BASE_API_ENDPOINT + self._depth_snapshot_path)
        self._feed.on_depth_snapshot(data=res)

    async def on_connect(self,
                         websocket: websockets.WebSocketClientProtocol) -> None:
        while True:
            try:
                res = json.loads(s=await websocket.recv())
                self._feed.on_websocket(data=res)
                if res.get('result') is None and res.get('id') == 1:
                    asyncio.create_task(coro=self.get_depth_snapshot())
            except websockets.ConnectionClosed as e:
                print(e)
                self.on_disconnect()
                await self.start()


class BybitWsClient(WsClient):
    _BASE_API_ENDPOINT = 'https://api.bybit.com'
    _api_auth: BybitApiAuth
    _pong_recv = False
    _ping_msg = json.dumps(obj={'op': 'ping'})

    def __init__(self, api_file_path: str, feed_object: feed.BybitFeed) -> None:
        self._api_auth = BybitApiAuth(file_path=api_file_path)
        sub_message = json.dumps(
            obj={'op': 'subscribe',
                 'args': ['orderBookL2_25.BTCUSD', 'order', 'execution',
                          'position']})
        super().__init__(sub_message=sub_message, feed_object=feed_object)

    async def start(self) -> None:
        print('START')
        await self.connect(uri=self._api_auth.get_websocket_uri(),
                           ping_interval=None)

    async def get_active_orders(self) -> None:
        res = await self.http_get(
            uri=(self._BASE_API_ENDPOINT
                 + self._api_auth.get_active_orders_auth(symbol='BTCUSD')))
        self._feed.on_order_snapshot(data=res)

    async def get_positions(self) -> None:
        res = await self.http_get(
            uri=(self._BASE_API_ENDPOINT
                 + self._api_auth.get_position_list_auth(symbol='BTCUSD')))
        self._feed.on_position_snapshot(data=res)

    async def on_connect(self,
                         websocket: websockets.WebSocketClientProtocol) -> None:
        heartbeat_t = asyncio.create_task(
            coro=self.heartbeat(websocket=websocket))
        while True:
            try:
                res = json.loads(s=await websocket.recv())
                if res.get('topic') is not None:
                    self._feed.on_websocket(data=res)
                elif (res.get('request').get('op') == 'subscribe'
                      and res.get('success') is True):
                    asyncio.create_task(coro=self.get_active_orders())
                    asyncio.create_task(coro=self.get_positions())
                elif res.get('ret_msg') == 'pong' and res.get('success'):
                    self._pong_recv = True
            except websockets.ConnectionClosed as e:
                print(e)
                heartbeat_t.cancel()
                await self.start()

    async def heartbeat(self,
                        websocket: websockets.WebSocketClientProtocol) -> None:
        while True:
            try:
                await websocket.send(message=self._ping_msg)
                await asyncio.sleep(delay=30)
                if not self._pong_recv:
                    await self.start()
                else:
                    self._pong_recv = False
            except websockets.ConnectionClosed as e:
                print(e)
                await self.start()
