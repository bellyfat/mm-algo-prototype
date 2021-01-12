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


def read_json_file(file_path: str) -> dict:
    with open(file=file_path) as fp:
        return json.load(fp=fp)


def get_signature(secret: str, message: str) -> str:
    return hmac.new(key=bytes(secret, encoding='utf8'),
                    msg=bytes(message, encoding='utf8'),
                    digestmod=hashlib.sha256).hexdigest()


def get_milli_timestamp() -> int:
    return time.time_ns() // 1000000


async def call_binance_listen_key(auth: dict,
                                  ssl_context: ssl.SSLContext) -> dict:
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'X-MBX-APIKEY': auth.get('id')}
    async with aiohttp.ClientSession() as session:
        timestamp = str(get_milli_timestamp())
        params = {'timestamp': timestamp,
                  'signature': get_signature(secret=auth.get('secret'),
                                             message=timestamp)}
        async with session.post(
                url='https://dapi.binance.com/dapi/v1/listenKey',
                data=urlencode(query=params), headers=headers,
                ssl=ssl_context) as res:
            return await res.json()


async def binance_listen_key_heartbeat_process(
        auth: dict,
        ssl_context: ssl.SSLContext) -> None:
    while True:
        await call_binance_listen_key(auth=auth, ssl_context=ssl_context)
        await asyncio.sleep(delay=300)


async def bybit_websocket_process(auth: dict,
                                  ssl_context: ssl.SSLContext) -> None:
    sub_msg = json.dumps(
        obj={'op': 'subscribe', 'args': ['orderBookL2_25.BTCUSD',
                                         'position', 'order', 'execution']})
    heartbeat_status = [False]
    expires = str(get_milli_timestamp() + 5000)
    params = {'api_key': auth.get('id'), 'expires': expires,
              'signature': get_signature(secret=auth.get('secret'),
                                         message='GET/realtime' + expires)}
    uri = 'wss://stream.bybit.com/realtime?' + urlencode(query=params)
    async with websockets.connect(uri=uri, ssl=ssl_context) as websocket:
        await websocket.send(message=sub_msg)
        asyncio.create_task(
            coro=bybit_heartbeat_process(heartbeat_status=heartbeat_status,
                                         websocket=websocket))
        while True:
            res = json.loads(s=await websocket.recv())
            topic = res.get('topic')
            if topic is not None:
                print(res)
            elif res.get('ret_msg') == 'pong' and res.get('success'):
                heartbeat_status[0] = True


async def bybit_heartbeat_process(
        heartbeat_status: list,
        websocket: websockets.WebSocketClientProtocol) -> None:
    ping_msg = json.dumps(obj={'op': 'ping'})
    while True:
        await websocket.send(message=ping_msg)
        await asyncio.sleep(delay=30)
        if not heartbeat_status[0]:
            raise Exception('No pong received, connection to Bybit is lost.')
        else:
            heartbeat_status[0] = False


async def binance_websocket_process(auth: dict,
                                    ssl_context: ssl.SSLContext) -> None:
    sub_msg = json.dumps(
        obj={'method': 'SUBSCRIBE', 'params': ['btcusd_perp@depth@100ms']})
    listen_key = (
        await call_binance_listen_key(auth=auth,
                                      ssl_context=ssl_context)).get('listenKey')
    uri = 'wss://dstream.binance.com/ws/' + listen_key
    async with websockets.connect(uri=uri, ssl=ssl_context) as websocket:
        await websocket.send(message=sub_msg)
        while True:
            res = json.loads(s=await websocket.recv())
            print(res)


async def main() -> None:
    ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    cert_pem = pathlib.Path('/etc/ssl/*').with_name(name='cert.pem')
    ssl_context.load_verify_locations(cafile=cert_pem)

    bybit_auth = read_json_file(file_path='../bybit_api_keys.json')
    binance_auth = read_json_file(file_path='../binance_api_keys.json')

    await asyncio.gather(
        bybit_websocket_process(auth=bybit_auth, ssl_context=ssl_context),
        binance_websocket_process(auth=binance_auth, ssl_context=ssl_context),
        binance_listen_key_heartbeat_process(auth=binance_auth,
                                             ssl_context=ssl_context))


asyncio.get_event_loop().run_until_complete(future=main())
