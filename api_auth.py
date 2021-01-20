from typing import Dict
import json
import hmac
import hashlib
import time
from urllib.parse import urlencode


def read_json_file(file_path: str) -> Dict[str, str]:
    with open(file=file_path) as fp:
        return json.load(fp=fp)


def get_signature(secret: str, message: str) -> str:
    return hmac.new(key=bytes(secret, encoding='utf8'),
                    msg=bytes(message, encoding='utf8'),
                    digestmod=hashlib.sha256).hexdigest()


def get_milli_timestamp() -> int:
    return time.time_ns() // 1000000


class ApiAuth:
    key: str
    secret: str

    def __init__(self, file_path: str) -> None:
        api_credentials = read_json_file(file_path=file_path)
        self.key = api_credentials.get('id')
        self.secret = api_credentials.get('secret')

    def get_signature(self, message) -> str:
        return get_signature(secret=self.secret, message=message)


class BinanceApiAuth(ApiAuth):
    headers: dict

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path=file_path)
        self.headers = {'Content-Type': 'application/x-www-form-urlencoded',
                        'X-MBX-APIKEY': self.key}

    def get_listen_key_data(self) -> str:
        params = {'timestamp': str(get_milli_timestamp())}
        params['signature'] = self.get_signature(
            message=urlencode(query=params))
        return urlencode(query=params)

    def get_open_orders_auth(self, symbol: str) -> str:
        params = {'symbol': symbol, 'timestamp': str(get_milli_timestamp())}
        params['signature'] = self.get_signature(
            message=urlencode(query=params))
        return '/dapi/v1/openOrders?' + urlencode(query=params)

    def get_position_risk_auth(self, pair: str) -> str:
        params = {'pair': pair, 'timestamp': str(get_milli_timestamp())}
        params['signature'] = self.get_signature(
            message=urlencode(query=params))
        return '/dapi/v1/positionRisk?' + urlencode(query=params)


class BybitApiAuth(ApiAuth):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path=file_path)

    def get_websocket_uri(self) -> str:
        expires = str(get_milli_timestamp() + 5000)
        params = {'api_key': self.key, 'expires': expires,
                  'signature': self.get_signature(
                      message='GET/realtime' + expires)}
        return 'wss://stream.bybit.com/realtime?' + urlencode(query=params)

    def get_active_orders_auth(self, symbol: str) -> str:
        params = {'api_key': self.key, 'symbol': symbol,
                  'timestamp': str(get_milli_timestamp())}
        params['sign'] = self.get_signature(message=urlencode(query=params))
        return '/v2/private/order?' + urlencode(query=params)

    def get_position_list_auth(self, symbol: str) -> str:
        params = {'api_key': self.key, 'symbol': symbol,
                  'timestamp': str(get_milli_timestamp())}
        params['sign'] = self.get_signature(message=urlencode(query=params))
        return '/v2/private/position/list?' + urlencode(query=params)
