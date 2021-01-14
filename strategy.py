import multiprocessing.connection
import multiprocessing as mp
from typing import Dict
from abc import abstractmethod


class Strategy:
    _feed_conns = Dict[str, mp.connection.Connection]

    def __init__(self, feed_conns: Dict[str, mp.connection.Connection]) -> None:
        self._feed_conns = feed_conns

    @abstractmethod
    def run_strategy(self) -> None:
        pass


class MMStrategy(Strategy):
    def __init__(self, feed_conns: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(feed_conns=feed_conns)

    def run_strategy(self) -> None:
        conn_bybit_bbo = self._feed_conns.get('bybit_bbo_chg')
        if conn_bybit_bbo.poll():
            data = conn_bybit_bbo.recv()
            print('BBO (Bybit):', str(data[0]), '@', str(data[1]))
        conn_binance_bbo = self._feed_conns.get('binance_bbo_chg')
        if conn_binance_bbo.poll():
            data = conn_binance_bbo.recv()
            print('BBO (Binance):', str(data[0]), '@', str(data[1]))
