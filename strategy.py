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
    _bybit_effective_prices = []
    _minimum_quotes = []
    _quote_targets = []
    _NET_FEE_OFFSET = 0.00015
    _bybit_eff_ticks = 3
    _bybit_tick_size = 0.5
    _bybit_symbol = 'BTCUSD'
    _binance_symbol = 'BTCUSD_PERP'
    _bybit_quote_size = 100
    is_bid_cancel_queued = [False]
    is_ask_cancel_queued = [False]
    is_bid_amend_queued = [False]
    is_ask_amend_queued = [False]

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
            elif order_status == 'Rejected' or order_status == 'Filled':
                self._bybit_active_orders.pop(ord_link_id)
            elif order_status == 'Cancelled':
                print('Cancelled')
                if ord_link_id in self._bybit_active_orders:
                    self._bybit_active_orders.pop(ord_link_id)
                self.on_cancel(order=order)

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

    def on_cancel(self, order: dict) -> None:
        if self._bybit_bid_ord_link_id == order.get('order_link_id'):
            self._bybit_bid_ord_link_id = None
            self.place_new_bybit_order(side='Buy')
        elif self._bybit_ask_ord_link_id == order.get('order_link_id'):
            self._bybit_ask_ord_link_id = None
            self.place_new_bybit_order(side='Sell')

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
                if self._bybit_position == 0:
                    self.place_new_bybit_order(side='Buy')
                    self.place_new_bybit_order(side='Sell')

    def on_sell_trade(self, execution: dict) -> None:
        hedge_qty = self.get_hedge_qty(execution=execution)
        if hedge_qty != 0:
            hedge_order = self.get_binance_new_market_order(side='BUY',
                                                            qty=hedge_qty)
            self._gateway.prepare_binance_new_order(order=hedge_order)
            if execution.get('leaves_qty') == 0:
                self._bybit_ask_ord_link_id = None
                print('FILLED SELL', self._bybit_position)
                if self._bybit_position == 0:
                    self.place_new_bybit_order(side='Buy')
                    self.place_new_bybit_order(side='Sell')

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

    def get_bybit_order_cancel(self, order_link_id: str) -> OrderedDict:
        return OrderedDict({'order_link_id': order_link_id,
                            'symbol': self._bybit_symbol})

    def place_new_bybit_order(self, side: str) -> None:
        if not self._gateway.is_rate_limited:
            if (side == 'Buy' and
                    self._quote_targets[0] >= self._bybit_effective_prices[0]):
                print('Place new order Bybit Buy')
                self._bybit_bid_ord_link_id = get_random_string(n=36)
                order = self.get_bybit_new_limit_order(
                    order_link_id=self._bybit_bid_ord_link_id,
                    price=self._quote_targets[0], side=side)
                self._gateway.prepare_bybit_new_order(order=order)
            elif (side == 'Sell' and
                  self._quote_targets[1] <= self._bybit_effective_prices[1]):
                print('Place new order Bybit Sell')
                self._bybit_ask_ord_link_id = get_random_string(n=36)
                order = self.get_bybit_new_limit_order(
                    order_link_id=self._bybit_ask_ord_link_id,
                    price=self._quote_targets[1], side=side)
                self._gateway.prepare_bybit_new_order(order=order)

    def compute_quote_targets(self) -> None:
        bybit_mid_price = np.mean(a=self._bybit_bbo)
        self._bybit_effective_prices = [
            bybit_mid_price - self._bybit_eff_ticks * self._bybit_tick_size,
            bybit_mid_price + self._bybit_eff_ticks * self._bybit_tick_size]

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
            self._quote_targets[1] = np.ceil(self._binance_bbo[0] * 2) / 2

    def check_new_quotes(self) -> None:
       # print('EFF:', self._bybit_effective_prices[0], '@',
              #self._bybit_effective_prices[1], '; BYBIT:', self._bybit_bbo[0], '@',
              #self._bybit_bbo[1], '; BINANCE:',
              #self._binance_bbo[0], '@', self._binance_bbo[1])
        if self._bybit_bid_ord_link_id is not None:
            order_local = self._bybit_active_orders.get(
                self._bybit_bid_ord_link_id)
            if (order_local is not None and not self.is_bid_amend_queued[0]
                    and order_local.get('price') != self._quote_targets[0]
                    and not self._gateway.is_rate_limited):
                if self._quote_targets[0] >= self._bybit_effective_prices[0]:
                    print('Amend Buy')
                    order = self.get_bybit_order_cancel_replace(
                        order_link_id=self._bybit_bid_ord_link_id,
                        p_r_price_=str(self._quote_targets[0]))
                    self._gateway.prepare_bybit_amend_order(
                        order=order, is_queued=self.is_bid_amend_queued)
                elif not self.is_bid_cancel_queued[0]:
                    print('Cancel Buy')
                    order = self.get_bybit_order_cancel(
                        order_link_id=self._bybit_bid_ord_link_id)
                    self._gateway.prepare_bybit_cancel_order(
                        order=order, is_queued=self.is_bid_cancel_queued)
        elif self._bybit_position <= 0:
            self.place_new_bybit_order(side='Buy')
        if self._bybit_ask_ord_link_id is not None:
            order_local = self._bybit_active_orders.get(
                self._bybit_ask_ord_link_id)
            if (order_local is not None and not self.is_ask_amend_queued[0]
                    and order_local.get('price') != self._quote_targets[1]
                    and not self._gateway.is_rate_limited):
                if self._quote_targets[1] <= self._bybit_effective_prices[1]:
                    print('Amend Sell')
                    order = self.get_bybit_order_cancel_replace(
                        order_link_id=self._bybit_ask_ord_link_id,
                        p_r_price_=str(self._quote_targets[1]))
                    self._gateway.prepare_bybit_amend_order(
                        order=order, is_queued=self.is_ask_amend_queued)
                elif not self.is_ask_cancel_queued[0]:
                    print('Cancel Sell')
                    order = self.get_bybit_order_cancel(
                        order_link_id=self._bybit_ask_ord_link_id)
                    self._gateway.prepare_bybit_cancel_order(
                        order=order, is_queued=self.is_ask_cancel_queued)
        elif self._bybit_position >= 0:
            self.place_new_bybit_order(side='Sell')