from typing import Tuple
from abc import abstractmethod
import numpy as np
from collections import OrderedDict
import random
import string
import gateway


def get_random_string(n):
    return ''.join(random.choice(seq=string.ascii_letters) for _ in range(n))


class Strategy:
    @abstractmethod
    def on_bybit_bbo_chg(self, data: Tuple[float, float]) -> None:
        pass

    @abstractmethod
    def on_binance_bbo_chg(self, data: Tuple[float, float]) -> None:
        pass

    @abstractmethod
    def on_bybit_order_update(self, data: dict) -> None:
        pass

    @abstractmethod
    def on_bybit_execution(self, data: dict) -> None:
        pass

    @abstractmethod
    def on_bybit_order_snap(self, data: dict) -> None:
        pass

    @abstractmethod
    def on_bybit_position_snap(self, data: dict) -> None:
        pass


class MMStrategy(Strategy):
    _gateway = gateway.Gateway
    _bybit_bbo = []
    _binance_bbo = []
    _bybit_active_orders = {}
    _bybit_bid_ord_link_id = None
    _bybit_ask_ord_link_id = None
    _bybit_position = 0
    _minimum_quotes = []
    _quote_targets = []
    _NET_FEE_OFFSET = 0.00015
    _NET_PROFIT_OFFSET = 0.00005
    _bybit_symbol = 'BTCUSD'
    _binance_symbol = 'BTCUSD_PERP'
    _bybit_quote_size = 100
    is_bid_amend_queued = [False]
    is_ask_amend_queued = [False]
    _inventory_limit = 1000
    _UPDATE_INTERVAL = 3
    _bid_update_count = 0
    _ask_update_count = 0

    def __init__(self, gateway: gateway.Gateway) -> None:
        self._gateway = gateway

    def on_bybit_bbo_chg(self, data: Tuple[float, float]) -> None:
        self._bybit_bbo = list(data)
        if len(self._binance_bbo) == 2:
            self.compute_quote_targets()
            self.check_new_quotes()

    def on_binance_bbo_chg(self, data: Tuple[float, float]) -> None:
        self._binance_bbo = list(data)
        if len(self._bybit_bbo) == 2:
            self.compute_quote_targets()
            self.check_new_quotes()

    def on_bybit_order_update(self, data: dict) -> None:
        orders = data.get('data')
        for order in orders:
            order_status = order.get('order_status')
            ord_link_id = order.get('order_link_id')
            if (order_status == 'Created' or order_status == 'New'
                    or order_status == 'PartiallyFilled'
                    or order_status == 'PendingCancel'):
                self._bybit_active_orders[ord_link_id] = order
            elif order_status == 'Filled':
                if ord_link_id in self._bybit_active_orders:
                    self._bybit_active_orders.pop(ord_link_id)
            elif order_status == 'Cancelled' or order_status == 'Rejected':
                print(order_status)
                if ord_link_id in self._bybit_active_orders:
                    self._bybit_active_orders.pop(ord_link_id)
                self.on_cancel_or_reject(order=order)

    def on_bybit_execution(self, data: dict) -> None:
        execs = data.get('data')
        for exec in execs:
            exec_side = exec.get('side')
            exec_qty = exec.get('exec_qty')
            exec_type = exec.get('exec_type')
            if exec_side == 'Buy':
                self._bybit_position += exec_qty
                if exec_type == 'Trade':
                    self.on_buy_trade(execution=exec)
            elif exec_side == 'Sell':
                self._bybit_position -= exec_qty
                if exec_type == 'Trade':
                    self.on_sell_trade(execution=exec)

    def on_bybit_order_snap(self, data: dict) -> None:
        self._bybit_active_orders = {}
        for order in data.get('result'):
            ord_link_id = order.get('order_link_id')
            self._bybit_active_orders[ord_link_id] = order

    def on_bybit_position_snap(self, data: dict) -> None:
        result: dict = data.get('result')
        size: int = result.get('size')
        side: str = result.get('side')
        self._bybit_position = (
            size if side == 'Long' or side == 'None' else -size)

    def on_cancel_or_reject(self, order: dict) -> None:
        if self._bybit_bid_ord_link_id == order.get('order_link_id'):
            self._bybit_bid_ord_link_id = None
        elif self._bybit_ask_ord_link_id == order.get('order_link_id'):
            self._bybit_ask_ord_link_id = None

    @staticmethod
    def get_hedge_qty(execution: dict) -> int:
        order_qty: int = execution.get('order_qty')
        exec_qty: int = execution.get('exec_qty')
        leaves_qty: int = execution.get('leaves_qty')
        pr_cum_qty = order_qty - leaves_qty - exec_qty
        exact_hedge_qty = (exec_qty + pr_cum_qty % 100) / 100
        return int(exact_hedge_qty - exact_hedge_qty % 1)

    def on_buy_trade(self, execution: dict) -> None:
        hedge_qty = self.get_hedge_qty(execution=execution)
        if hedge_qty != 0:
            hedge_order = self.get_binance_new_market_order(side='SELL',
                                                            qty=hedge_qty)
            self._gateway.prepare_binance_new_order(order=hedge_order)
            if execution.get('leaves_qty') == 0:
                self._bybit_bid_ord_link_id = None
                print('FILLED BUY', self._bybit_position)

    def on_sell_trade(self, execution: dict) -> None:
        hedge_qty = self.get_hedge_qty(execution=execution)
        if hedge_qty != 0:
            hedge_order = self.get_binance_new_market_order(side='BUY',
                                                            qty=hedge_qty)
            self._gateway.prepare_binance_new_order(order=hedge_order)
            if execution.get('leaves_qty') == 0:
                self._bybit_ask_ord_link_id = None
                print('FILLED SELL', self._bybit_position)

    def get_bybit_new_limit_order(self, order_link_id: str, price: float,
                                  side: str) -> OrderedDict:
        return OrderedDict({'order_link_id': order_link_id,
                            'order_type': 'Limit', 'price': price,
                            'qty': self._bybit_quote_size, 'side': side,
                            'symbol': self._bybit_symbol,
                            'time_in_force': 'PostOnly'})

    def get_binance_new_market_order(self, side: str, qty: int) -> OrderedDict:
        return OrderedDict({'symbol': self._binance_symbol, 'side': side,
                            'type': 'MARKET', 'quantity': qty})

    def get_bybit_order_cancel_replace(self, order_link_id: str,
                                       p_r_price_: str) -> OrderedDict:
        return OrderedDict({'order_link_id': order_link_id,
                            'p_r_price': p_r_price_,
                            'symbol': self._bybit_symbol})

    def place_new_bybit_order(self, side: str) -> None:
        if not self._gateway.is_rate_limited:
            if side == 'Buy' and self._bybit_bid_ord_link_id is None:
                print('Place new order buy (Bybit)')
                self._bybit_bid_ord_link_id = get_random_string(n=36)
                order = self.get_bybit_new_limit_order(
                    order_link_id=self._bybit_bid_ord_link_id,
                    price=self._quote_targets[0], side=side)
                self._gateway.prepare_bybit_new_order(order=order)
            elif side == 'Sell' and self._bybit_ask_ord_link_id is None:
                print('Place new order sell (Bybit)')
                self._bybit_ask_ord_link_id = get_random_string(n=36)
                order = self.get_bybit_new_limit_order(
                    order_link_id=self._bybit_ask_ord_link_id,
                    price=self._quote_targets[1], side=side)
                self._gateway.prepare_bybit_new_order(order=order)

    def compute_quote_targets(self) -> None:
        self._quote_targets.clear()
        self._quote_targets.append(np.floor(
            (1 - self._NET_FEE_OFFSET - self._NET_PROFIT_OFFSET)
            * min((self._bybit_bbo[0], self._binance_bbo[0])) * 2) / 2)
        self._quote_targets.append(np.ceil(
            (1 + self._NET_FEE_OFFSET + self._NET_PROFIT_OFFSET)
            * max((self._bybit_bbo[1], self._binance_bbo[1])) * 2) / 2)

    def check_new_quotes(self) -> None:
        if self._bybit_bid_ord_link_id is not None:
            order_local = self._bybit_active_orders.get(
                self._bybit_bid_ord_link_id)
            if (order_local is not None and not self.is_bid_amend_queued[0]
                    and order_local.get('price') != self._quote_targets[0]
                    and not self._gateway.is_rate_limited):
                self._bid_update_count += 1
                if self._bid_update_count == self._UPDATE_INTERVAL:
                    print('Amend Buy', len(self._bybit_active_orders))
                    order = self.get_bybit_order_cancel_replace(
                        order_link_id=self._bybit_bid_ord_link_id,
                        p_r_price_=str(self._quote_targets[0]))
                    self._gateway.prepare_bybit_amend_order(
                        order=order, is_queued=self.is_bid_amend_queued)
                    self._bid_update_count = 0
        elif (self._bybit_position <=
              self._inventory_limit - self._bybit_quote_size):
            self.place_new_bybit_order(side='Buy')
        if self._bybit_ask_ord_link_id is not None:
            order_local = self._bybit_active_orders.get(
                self._bybit_ask_ord_link_id)
            if (order_local is not None and not self.is_ask_amend_queued[0]
                    and order_local.get('price') != self._quote_targets[1]
                    and not self._gateway.is_rate_limited):
                self._ask_update_count += 1
                if self._ask_update_count == self._UPDATE_INTERVAL:
                    print('Amend Sell', len(self._bybit_active_orders))
                    order = self.get_bybit_order_cancel_replace(
                        order_link_id=self._bybit_ask_ord_link_id,
                        p_r_price_=str(self._quote_targets[1]))
                    self._gateway.prepare_bybit_amend_order(
                        order=order, is_queued=self.is_ask_amend_queued)
                    self._ask_update_count = 0
        elif (self._bybit_position >=
              -self._inventory_limit + self._bybit_quote_size):
            self.place_new_bybit_order(side='Sell')
