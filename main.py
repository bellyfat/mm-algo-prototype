from feed import BybitFeed, BinanceFeed
from ws_client import BybitWsClient, BinanceWsClient
import strategy
import asyncio
from typing import Coroutine
import gateway

API_KEY_PATH_BYBIT = '../bybit_api_keys.json'
API_KEY_PATH_BINANCE = '../binance_api_keys.json'


async def run_async(*args: Coroutine) -> None:
    await asyncio.gather(*args)

if __name__ == '__main__':
    gw = gateway.Gateway(api_pth_bybit=API_KEY_PATH_BYBIT,
                         api_pth_binance=API_KEY_PATH_BINANCE)
    strategy = strategy.MMStrategy(gateway=gw)
    bybit_feed = BybitFeed(strat=strategy)
    binance_feed = BinanceFeed(strat=strategy)
    bybit_ws_client = BybitWsClient(api_file_path=API_KEY_PATH_BYBIT,
                                    feed_object=bybit_feed)
    binance_ws_client = BinanceWsClient(api_file_path=API_KEY_PATH_BINANCE,
                                        feed_object=binance_feed)
    asyncio.get_event_loop().run_until_complete(
        future=run_async(bybit_ws_client.start(), binance_ws_client.start()))
