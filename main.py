from feed import Feed, BybitFeed, BinanceFeed
from ws_client import BybitWsClient, BinanceWsClient
import multiprocessing as mp
import asyncio
from typing import Dict, List, Coroutine, Tuple, Union

API_KEY_PATH_BYBIT = '../bybit_api_keys.json'
API_KEY_PATH_BINANCE = '../binance_api_keys.json'


def get_pipes(
        pipe_names: Union[List, Tuple]) -> Dict[str, mp.connection.Connection]:
    return {name: mp.Pipe(duplex=False) for name in pipe_names}


def get_conns(pipes: Dict[str, mp.Pipe],
              index: int) -> Dict[str, mp.connection.Connection]:
    return {key: value[index] for key, value in pipes.items()}


def run_feeds(*args: Feed) -> None:
    while True:
        for arg in args:
            arg.process_feed()


async def run_async(*args: Coroutine) -> None:
    await asyncio.gather(*args)

if __name__ == '__main__':
    bybit_pipes = get_pipes(pipe_names=('websocket_stream',))
    binance_pipes = get_pipes(pipe_names=('websocket_stream', 'depth_snapshot'))

    bybit_feed = BybitFeed(pipe=get_conns(pipes=bybit_pipes, index=0))
    binance_feed = BinanceFeed(pipe=get_conns(pipes=binance_pipes, index=0))

    bybit_ws_client = BybitWsClient(api_file_path=API_KEY_PATH_BYBIT,
                                    pipe=get_conns(pipes=bybit_pipes, index=1))
    binance_ws_client = BinanceWsClient(api_file_path=API_KEY_PATH_BINANCE,
                                        pipe=get_conns(pipes=binance_pipes,
                                                       index=1))

    mp.Process(target=run_feeds, args=(bybit_feed, binance_feed)).start()
    asyncio.get_event_loop().run_until_complete(
        future=run_async(bybit_ws_client.start(), binance_ws_client.start()))
