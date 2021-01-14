import asyncio
import ssl
import certifi
from abc import abstractmethod
import websockets
import json
import aiohttp
import multiprocessing.connection
import multiprocessing as mp
from typing import Dict
from api_auth import BybitApiAuth, BinanceApiAuth


class WsClient:
    _ssl_context: ssl.SSLContext
    _sub_message: str
    _pipe: Dict[str, mp.connection.Connection]

    def __init__(self, sub_message: str,
                 pipe: Dict[str, mp.connection.Connection]) -> None:
        self._ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
        self. _ssl_context.load_verify_locations(cafile=certifi.where())
        self._sub_message = sub_message
        self._pipe = _pipe = pipe

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def on_connect(self,
                         websocket: websockets.WebSocketClientProtocol) -> None:
        pass

    async def connect(self, uri: str) -> None:
        async with websockets.connect(uri=uri,
                                      ssl=self._ssl_context) as websocket:
            await websocket.send(message=self._sub_message)
            await self.on_connect(websocket=websocket)

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
    _SYMBOL = 'BTCUSD_PERP'
    _is_depth_snap_req = False
    _depth_snapshot_path = '/dapi/v1/depth?symbol={}&limit=1000'.format(_SYMBOL)

    def __init__(self, api_file_path: str,
                 pipe: Dict[str, mp.connection.Connection]) -> None:
        self._api_auth = BinanceApiAuth(file_path=api_file_path)
        sub_message = json.dumps(
            obj={'method': 'SUBSCRIBE', 'params': ['btcusd_perp@depth@100ms']})
        super().__init__(sub_message=sub_message, pipe=pipe)

    async def start(self) -> None:
        listen_key = (await self.call_listen_key()).get('listenKey')
        asyncio.create_task(coro=self.listen_key_heartbeat())
        await self.connect(uri='wss://dstream.binance.com/ws/' + listen_key)

    async def call_listen_key(self) -> dict:
        return await self.http_post(
            uri=self._BASE_API_ENDPOINT + '/dapi/v1/listenKey',
            data=self._api_auth.get_listen_key_data(),
            headers=self._api_auth.headers)

    async def listen_key_heartbeat(self) -> None:
        while True:
            await asyncio.sleep(delay=1800)
            await self.call_listen_key()

    async def get_depth_snapshot(self) -> None:
        res = await self.http_get(
            uri=self._BASE_API_ENDPOINT + self._depth_snapshot_path)
        self._pipe.get('depth_snapshot').send(obj=res)

    async def on_connect(self,
                         websocket: websockets.WebSocketClientProtocol) -> None:
        while True:
            res = json.loads(s=await websocket.recv())
            self._pipe.get('websocket_stream').send(obj=res)
            if not self._is_depth_snap_req and res.get('e') == 'depthUpdate':
                asyncio.create_task(coro=self.get_depth_snapshot())
                self._is_depth_snap_req = True


class BybitWsClient(WsClient):
    _api_auth: BybitApiAuth
    _pong_recv = False
    _ping_msg = json.dumps(obj={'op': 'ping'})

    def __init__(self, api_file_path: str,
                 pipe: Dict[str, mp.connection.Connection]) -> None:
        self._api_auth = BybitApiAuth(file_path=api_file_path)
        sub_message = json.dumps(
            obj={'op': 'subscribe',
                 'args': ['orderBookL2_25.BTCUSD', 'position', 'order',
                          'execution']})
        super().__init__(sub_message=sub_message, pipe=pipe)

    async def start(self) -> None:
        await self.connect(uri=self._api_auth.get_websocket_uri())

    async def on_connect(self,
                         websocket: websockets.WebSocketClientProtocol) -> None:
        asyncio.create_task(coro=self.heartbeat(websocket=websocket))
        while True:
            res = json.loads(s=await websocket.recv())
            if res.get('topic') is not None:
                self._pipe.get('websocket_stream').send(obj=res)
            elif res.get('ret_msg') == 'pong' and res.get('success'):
                self._pong_recv = True

    async def heartbeat(self,
                        websocket: websockets.WebSocketClientProtocol) -> None:
        while True:
            await websocket.send(message=self._ping_msg)
            await asyncio.sleep(delay=30)
            if not self._pong_recv:
                raise Exception('Bybit: no pong received.')
            else:
                self._pong_recv = False

