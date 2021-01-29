from feed import Feed, BybitFeed, BinanceFeed
from ws_client import BybitWsClient, BinanceWsClient
from strategy import Strategy, MMStrategy
import multiprocessing as mp
import asyncio
from typing import Dict, List, Coroutine, Tuple, Union
from gateway import Gateway

API_KEY_PATH_BYBIT = '../bybit_api_keys.json'
API_KEY_PATH_BINANCE = '../binance_api_keys.json'


def get_pipes(
        pipe_names: Union[List, Tuple]) -> Dict[str, mp.connection.Connection]:
    return {name: mp.Pipe(duplex=False) for name in pipe_names}


def get_conns(pipes: Dict[str, mp.Pipe],
              index: int) -> Dict[str, mp.connection.Connection]:
    return {key: value[index] for key, value in pipes.items()}


def process_feeds(*args: Feed) -> None:
    while True:
        for arg in args:
            arg.process_feed()


def run_strategies(*args: Strategy) -> None:
    while True:
        for arg in args:
            arg.run_strategy()


def run_gateway(arg: Gateway) -> None:
    while True:
        arg.run_gateway()


async def run_async(*args: Coroutine) -> None:
    await asyncio.gather(*args)

if __name__ == '__main__':
    bybit_pipes = get_pipes(
        pipe_names=('ws-stream', 'order-snap', 'position-snap'))
    binance_pipes = get_pipes(
        pipe_names=('book-reset', 'ws-stream', 'depth-snap',
                    'order-snap', 'position-snap'))

    strategy_pipes = get_pipes(
        pipe_names=('bybit-bbo-chg', 'binance-bbo-chg', 'bybit-order-update',
                    'bybit-execution', 'binance-order-trade-update',
                    'bybit-order-snap', 'binance-order-snap', 'bybit-position',
                    'binance-account-update', 'bybit-position-snap',
                    'binance-position-snap'))

    gateway_pipes = get_pipes(
        pipe_names=('binance-new-order', 'bybit-new-order', 'bybit-amend'))

    bybit_feed = BybitFeed(
        ws_conns=get_conns(pipes=bybit_pipes, index=0),
        strategy_conns=get_conns(pipes=strategy_pipes, index=1))
    binance_feed = BinanceFeed(
        ws_conns=get_conns(pipes=binance_pipes, index=0),
        strategy_conns=get_conns(pipes=strategy_pipes, index=1))

    strategy = MMStrategy(feed_conns=get_conns(pipes=strategy_pipes, index=0),
                          gateway_conns=get_conns(pipes=gateway_pipes, index=1))
    gateway = Gateway(api_pth_bybit=API_KEY_PATH_BYBIT,
                      api_pth_binance=API_KEY_PATH_BINANCE,
                      strategy_conns=get_conns(pipes=gateway_pipes, index=0))

    bybit_ws_client = BybitWsClient(
        api_file_path=API_KEY_PATH_BYBIT,
        feed_conns=get_conns(pipes=bybit_pipes, index=1))
    binance_ws_client = BinanceWsClient(
        api_file_path=API_KEY_PATH_BINANCE,
        feed_conns=get_conns(pipes=binance_pipes, index=1))

    mp.Process(target=process_feeds, args=(bybit_feed, binance_feed)).start()
    mp.Process(target=run_strategies, args=(strategy,)).start()
    mp.Process(target=run_gateway, args=(gateway,)).start()
    asyncio.get_event_loop().run_until_complete(
        future=run_async(bybit_ws_client.start(), binance_ws_client.start()))
