import asyncio
import ssl
import pathlib
import websockets
import time
import json
from urllib.parse import urlencode
import hmac
import hashlib
import aiohttp

ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
cert_pem = pathlib.Path('/etc/ssl/*').with_name(name='cert.pem')
ssl_context.load_verify_locations(cafile=cert_pem)

bybit_sub_msg = json.dumps(
    obj={'op': 'subscribe', 'args': ['orderBookL2_25.BTCUSD',
                                     'position', 'order', 'execution']})
bybit_ping_msg = json.dumps(obj={'op': 'ping'})
binance_sub_msg = json.dumps(
    obj={'method': 'SUBSCRIBE', 'params': ['btcusd_perp@depth@100ms']})


def read_json_file(file_path: str) -> dict:
    with open(file=file_path) as fp:
        return json.load(fp=fp)


bybit_api_keys = read_json_file(file_path='../bybit_api_keys.json')
binance_api_keys = read_json_file(file_path='../binance_api_keys.json')

binance_http_headers = {'Content-Type': 'application/x-www-form-urlencoded',
                        'X-MBX-APIKEY': binance_api_keys.get('id')}


def get_signature(secret: str, message: str) -> str:
    return hmac.new(key=bytes(secret, encoding='utf8'),
                    msg=bytes(message, encoding='utf8'),
                    digestmod=hashlib.sha256).hexdigest()


def get_milli_timestamp() -> int:
    return time.time_ns() // 1000000


def get_bybit_connect_uri() -> str:
    expires = str(get_milli_timestamp() + 5000)
    params = {'api_key': bybit_api_keys.get('id'), 'expires': expires,
              'signature': get_signature(secret=bybit_api_keys.get('secret'),
                                         message='GET/realtime' + expires)}
    return 'wss://stream.bybit.com/realtime?' + urlencode(query=params)


async def call_binance_listen_key() -> dict:
    async with aiohttp.ClientSession() as session:
        timestamp = str(get_milli_timestamp())
        params = {'timestamp': timestamp,
                  'signature': get_signature(
                      secret=binance_api_keys.get('secret'), message=timestamp)}
        async with session.post(
                url='https://dapi.binance.com/dapi/v1/listenKey',
                data=urlencode(query=params), headers=binance_http_headers,
                ssl=ssl_context) as res:
            return await res.json()


async def binance_listen_key_heartbeat_process():
    while True:
        await call_binance_listen_key()
        await asyncio.sleep(delay=300)


async def bybit_websocket_process() -> None:
    expires = str(get_milli_timestamp() + 5000)
    params = {'api_key': bybit_api_keys.get('id'), 'expires': expires,
              'signature': get_signature(secret=bybit_api_keys.get('secret'),
                                         message='GET/realtime' + expires)}
    uri = 'wss://stream.bybit.com/realtime?' + urlencode(query=params)
    async with websockets.connect(uri=uri, ssl=ssl_context) as websocket:
        await websocket.send(message=bybit_sub_msg)
        asyncio.create_task(coro=bybit_heartbeat_process(websocket=websocket))
        while True:
            res = await websocket.recv()
            print(json.loads(s=res))


async def bybit_heartbeat_process(
        websocket: websockets.WebSocketClientProtocol) -> None:
    while True:
        await websocket.send(message=bybit_ping_msg)
        await asyncio.sleep(delay=30)


async def binance_websocket_process() -> None:
    listen_key = (await call_binance_listen_key()).get('listenKey')
    uri = 'wss://dstream.binance.com/ws/' + listen_key
    async with websockets.connect(uri=uri, ssl=ssl_context) as websocket:
        await websocket.send(message=binance_sub_msg)
        while True:
            res = await websocket.recv()
            print(json.loads(s=res))


async def main() -> None:
    await asyncio.gather(bybit_websocket_process(), binance_websocket_process(),
                         binance_listen_key_heartbeat_process())


asyncio.get_event_loop().run_until_complete(future=main())
