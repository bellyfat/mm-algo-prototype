import multiprocessing.connection
import multiprocessing as mp
from typing import Dict
from abc import abstractmethod
import numpy as np


class Strategy:
    _feed_conns = Dict[str, mp.connection.Connection]

    def __init__(self, feed_conns: Dict[str, mp.connection.Connection]) -> None:
        self._feed_conns = feed_conns

    @abstractmethod
    def run_strategy(self) -> None:
        pass


class MMStrategy(Strategy):
    _bybit_bbo = []
    _binance_bbo = []
    _minimum_quotes = []
    _quote_targets = []
    _NET_FEE_OFFSET = 0.00015

    def __init__(self, feed_conns: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(feed_conns=feed_conns)

    def run_strategy(self) -> None:
        for key, conn in self._feed_conns.items():
            if conn.poll():
                data = conn.recv()
                if key == 'bybit_bbo_chg':
                    self._bybit_bbo = list(data)
                    if len(self._binance_bbo) == 2:
                        self.compute_quotes()
                elif key == 'binance_bbo_chg':
                    self._binance_bbo = list(data)
                    if len(self._bybit_bbo) == 2:
                        self.compute_quotes()
                elif key == 'bybit_order':
                    print(data)
                elif key == 'bybit_execution':
                    print(data)
                elif key == 'binance_order_trade_upd':
                    print(data)
                elif key == 'bybit_active_orders':
                    print(data)
                elif key == 'binance_open_orders':
                    print(data)
                elif key == 'bybit_position':
                    print(data)
                elif key == 'binance_position':
                    print(data)

    def compute_quotes(self) -> None:
        average_price = np.mean(a=self._bybit_bbo + self._binance_bbo)
        self._minimum_quotes = [
            np.floor((1 - self._NET_FEE_OFFSET) * average_price * 2) / 2,
            np.ceil((1 + self._NET_FEE_OFFSET) * average_price * 2) / 2]
        self._quote_targets = self._minimum_quotes
        # Check if maximum bid is above current Bybit best bid
        if self._minimum_quotes[0] > self._bybit_bbo[0]:
            # Adjust quoted bid to current Bybit best bid
            self._quote_targets[0] = self._bybit_bbo[0]

        # Else, check if maximum bid is still above best offer on Binance
        # (If we get hit on the bid, we want to take the Binance offer)
        elif self._minimum_quotes[0] > self._binance_bbo[1]:
            # Adjust quoted bid to current Binance best offer
            self._quote_targets[0] = np.floor(self._binance_bbo[1] * 2) / 2
        # Check if minimum offer is below current Bybit best offer
        if self._minimum_quotes[1] < self._bybit_bbo[1]:
            # Adjust quoted offer to current Bybit best offer
            self._quote_targets[1] = self._bybit_bbo[1]

        # Else, check if minimum offer is still below best bid on Binance
        # (If we get lifted on the offer, we want to take the Binance bid)
        elif self._minimum_quotes[1] < self._binance_bbo[0]:
            # Adjust quoted offer to current Binance best bid
            self._minimum_quotes[1] = np.ceil(self._binance_bbo[0] * 2) / 2
        #print('QUOTES:', self._quote_targets[0], '@', self._quote_targets[1],
              #';', 'BINANCE:',  self._binance_bbo[0], '@', self._binance_bbo[1],
              #';', 'BYBIT:',  self._bybit_bbo[0], '@', self._bybit_bbo[1])
        #print('BYBIT - BINANCE:', np.round(np.mean(a=self._bybit_bbo) - np.mean(self._binance_bbo)))

