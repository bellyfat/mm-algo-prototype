from feed import Feed, BybitFeed, BinanceFeed
from ws_client import BybitWsClient, BinanceWsClient
import multiprocessing as mp
import asyncio
from typing import List, Coroutine


def run_feeds(*args: Feed) -> None:
    while True:
        for arg in args:
            arg.process_feed()


async def run_async(*args: Coroutine) -> None:
    await asyncio.gather(*args)

if __name__ == '__main__':
    bybit_pipes = {'websocket_stream': mp.Pipe(duplex=False)}
    binance_pipes = {'websocket_stream': mp.Pipe(duplex=False),
                     'depth_snapshot': mp.Pipe(duplex=False)}

    bybit_feed = BybitFeed(pipe={key: value[0]
                                 for key, value in bybit_pipes.items()})
    binance_feed = BinanceFeed(pipe={key: value[0]
                                     for key, value in binance_pipes.items()})

    bybit_ws_client = BybitWsClient(
        api_file_path='../bybit_api_keys.json',
        pipe={key: value[1] for key, value in bybit_pipes.items()})
    binance_ws_client = BinanceWsClient(
        api_file_path='../binance_api_keys.json',
        pipe={key: value[1] for key, value in binance_pipes.items()})

    mp.Process(target=run_feeds, args=(bybit_feed, binance_feed)).start()
    asyncio.get_event_loop().run_until_complete(
        future=run_async(bybit_ws_client.start(), binance_ws_client.start()))
