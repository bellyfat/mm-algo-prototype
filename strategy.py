from typing import Tuple, Union
from abc import abstractmethod
import numpy as np
from collections import OrderedDict
import random
import string
from gateway import Gateway


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

    @abstractmethod
    def on_binance_position_snap(self, data: dict) -> None:
        pass


class MMStrategy(Strategy):
    _gateway = Gateway
    _bybit_bbo = []
    _binance_bbo = []
    _bybit_active_orders = {}
    _bybit_bid_ord_link_id = None
    _bybit_ask_ord_link_id = None
    _bybit_position = None
    _binance_position = None
    _quote_targets = []
    _NET_FEE_OFFSET = 0.00015
    _NET_PROFIT_OFFSET = 0.00005
    _RISK_MEASURE = 0.00015
    _bybit_symbol = 'BTCUSD'
    _binance_symbol = 'BTCUSD_PERP'
    _bybit_quote_size = 100
    _is_order_op_queued = [False]
    _inventory_limit = 50000
    _UPDATE_INTERVAL = 3
    _bid_update_count = 0
    _ask_update_count = 0
    _bybit_unhedged_qty = 0

    def __init__(self, gateway: Gateway) -> None:
        self._gateway = gateway

    def on_bybit_bbo_chg(self, data: Tuple[float, float]) -> None:
        self._bybit_bbo = list(data)
        if (len(self._binance_bbo) == 2 and self._bybit_position is not None
                and self._binance_position is not None):
            self.compute_quote_targets()
            self.check_new_quotes()

    def on_binance_bbo_chg(self, data: Tuple[float, float]) -> None:
        self._binance_bbo = list(data)
        if (len(self._binance_bbo) == 2 and self._bybit_position is not None
                and self._binance_position is not None):
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
                else:
                    print('Cancellation not in active orders')
                self.on_cancel_or_reject(ord_link_id=ord_link_id)

    def on_bybit_execution(self, data: dict) -> None:
        execs = data.get('data')
        for execution in execs:
            exec_side = execution.get('side')
            exec_qty = execution.get('exec_qty')
            exec_type = execution.get('exec_type')
            if exec_side == 'Buy':
                self._bybit_position += exec_qty
                if exec_type == 'Trade':
                    self.on_buy_trade(execution=execution)
            elif exec_side == 'Sell':
                self._bybit_position -= exec_qty
                if exec_type == 'Trade':
                    self.on_sell_trade(execution=execution)

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
            size if side == 'Buy' or side == 'None' else -size)
        if self._binance_position is not None:
            self._bybit_unhedged_qty = (
                    self._bybit_position + 100 * self._binance_position)

    def on_binance_position_snap(self, data: dict) -> None:
        amt = int(data.get('positionAmt'))
        side: str = data.get('positionSide')
        self._binance_position = (
            amt if side == 'LONG' or side == 'BOTH' else -amt)
        if self._bybit_position is not None:
            self._bybit_unhedged_qty = (
                    self._bybit_position + 100 * self._binance_position)

    def on_cancel_or_reject(self, ord_link_id: str) -> None:
        if self._bybit_bid_ord_link_id == ord_link_id:
            print('Bid cancelled/rejected')
            self._bybit_bid_ord_link_id = None
        elif self._bybit_ask_ord_link_id == ord_link_id:
            print('Ask cancelled/rejected')
            self._bybit_ask_ord_link_id = None
        else:
            print('Unknown order link id cancelled/rejected')

    def check_hedge(self, exec_qty: int) -> None:
        total_unhedged_qty = self._bybit_unhedged_qty + exec_qty
        hedge_contracts = round(total_unhedged_qty / 100)
        self._bybit_unhedged_qty = total_unhedged_qty - hedge_contracts * 100
        if hedge_contracts != 0:
            self.hedge_binance(contracts=hedge_contracts)

    def hedge_binance(self, contracts: int) -> None:
        if contracts > 0:
            hedge_order = self.get_binance_new_market_order(side='SELL',
                                                            qty=contracts)
            self._gateway.prepare_binance_new_order(order=hedge_order)
            self._binance_position -= contracts
            print('HEDGE SELL:', contracts)
        elif contracts < 0:
            hedge_order = self.get_binance_new_market_order(side='BUY',
                                                            qty=abs(contracts))
            self._gateway.prepare_binance_new_order(order=hedge_order)
            self._binance_position += abs(contracts)
            print('HEDGE BUY:', abs(contracts))

    def on_buy_trade(self, execution: dict) -> None:
        self.check_hedge(exec_qty=execution.get('exec_qty'))
        if execution.get('leaves_qty') == 0:
            self._bybit_bid_ord_link_id = None
            print('FILLED BUY', self._bybit_position)

    def on_sell_trade(self, execution: dict) -> None:
        self.check_hedge(exec_qty=-execution.get('exec_qty'))
        if execution.get('leaves_qty') == 0:
            self._bybit_ask_ord_link_id = None
            print('FILLED SELL', self._bybit_position)

    def get_bybit_new_limit_order(self, order_link_id: str, price: float,
                                  qty: int, side: str) -> OrderedDict:
        return OrderedDict({'order_link_id': order_link_id,
                            'order_type': 'Limit', 'price': price,
                            'qty': qty, 'side': side,
                            'symbol': self._bybit_symbol,
                            'time_in_force': 'PostOnly'})

    def get_binance_new_market_order(self, side: str, qty: int) -> OrderedDict:
        return OrderedDict({'symbol': self._binance_symbol, 'side': side,
                            'type': 'MARKET', 'quantity': qty})

    def get_bybit_order_cancel_replace(self, order_link_id: str,
                                       p_r_price_: str,
                                       p_r_qty=None) -> OrderedDict:
        if p_r_qty is not None:
            return OrderedDict({'order_link_id': order_link_id,
                                'p_r_price': p_r_price_,
                                'p_r_qty': str(p_r_qty),
                                'symbol': self._bybit_symbol})
        else:
            return OrderedDict({'order_link_id': order_link_id,
                                'p_r_price': p_r_price_,
                                'symbol': self._bybit_symbol})

    def place_new_bybit_order(self, side: str) -> None:
        if not self._gateway.is_rate_limited:
            if side == 'Buy' and self._bybit_bid_ord_link_id is None:
                if not self._is_order_op_queued[0]:
                    order_size = self.get_order_size(side='Buy')
                    if order_size != 0:
                        print('Placed new buy limit')
                        self._bybit_bid_ord_link_id = get_random_string(n=36)
                        order = self.get_bybit_new_limit_order(
                            order_link_id=self._bybit_bid_ord_link_id,
                            price=self._quote_targets[0], side=side,
                            qty=order_size)
                        self._gateway.prepare_bybit_new_order(
                            order=order, is_queued=self._is_order_op_queued)
                    else:
                        print('Buy order size 0, no order placed')
                else:
                    print('Buy order op queued')
            elif side == 'Sell' and self._bybit_ask_ord_link_id is None:
                if not self._is_order_op_queued[0]:
                    order_size = self.get_order_size(side='Sell')
                    if order_size != 0:
                        print('Placed new sell limit')
                        self._bybit_ask_ord_link_id = get_random_string(n=36)
                        order = self.get_bybit_new_limit_order(
                            order_link_id=self._bybit_ask_ord_link_id,
                            price=self._quote_targets[1], side=side,
                            qty=order_size)
                        self._gateway.prepare_bybit_new_order(
                            order=order, is_queued=self._is_order_op_queued)
                    else:
                        print('Sell order size 0, no order placed')
                else:
                    print('Sell order op queued')

    def compute_quote_targets(self) -> None:
        bybit_mid = np.mean(a=self._bybit_bbo)
        binance_mid = np.mean(a=self._binance_bbo)
        overall_mid = np.mean(a=(bybit_mid, binance_mid))
        minimum_quotes = [
            np.floor((1 - self._NET_FEE_OFFSET - self._NET_PROFIT_OFFSET
                      - self._RISK_MEASURE) * overall_mid * 2) / 2,
            np.ceil((1 + self._NET_FEE_OFFSET + self._NET_PROFIT_OFFSET
                     + self._RISK_MEASURE) * overall_mid * 2) / 2]
        if bybit_mid < binance_mid:
            max_bid = self._bybit_bbo[1] - 0.5
            if max_bid < minimum_quotes[0]:
                minimum_quotes[0] = max_bid
            if self._binance_bbo[1] > minimum_quotes[1]:
                minimum_quotes[1] = np.ceil(self._binance_bbo[1] * 2) / 2
        elif bybit_mid > binance_mid:
            min_ask = self._bybit_bbo[0] + 0.5
            if min_ask > minimum_quotes[1]:
                minimum_quotes[1] = min_ask
            if self._binance_bbo[0] < minimum_quotes[0]:
                minimum_quotes[0] = np.floor(self._binance_bbo[0] * 2) / 2
        self._quote_targets = minimum_quotes

    def get_order_size(self, side: str) -> int:
        if side == 'Buy':
            if self._bybit_position < 0:
                return abs(self._bybit_position)
            else:
                rmd = self._bybit_position % self._bybit_quote_size
                if rmd == 0:
                    if (self._bybit_position + self._bybit_quote_size
                            <= self._inventory_limit):
                        return self._bybit_quote_size
                else:
                    order_size = self._bybit_quote_size - rmd
                    if (self._bybit_position + order_size
                            + self._bybit_quote_size <= self._inventory_limit):
                        order_size += self._bybit_quote_size
                    return order_size
        elif side == 'Sell':
            if self._bybit_position > 0:
                return self._bybit_position
            else:
                rmd = abs(self._bybit_position) % self._bybit_quote_size
                if rmd == 0:
                    if (self._bybit_position - self._bybit_quote_size
                            >= -self._inventory_limit):
                        return self._bybit_quote_size
                else:
                    order_size = self._bybit_quote_size - rmd
                    if (self._bybit_position - order_size
                            - self._bybit_quote_size >= -self._inventory_limit):
                        order_size += self._bybit_quote_size
                    return order_size
        return 0

    def check_new_quotes(self) -> None:
        if self._bybit_bid_ord_link_id is not None:
            order_local = self._bybit_active_orders.get(
                self._bybit_bid_ord_link_id)
            if (order_local is not None and not self._is_order_op_queued[0]
                    and order_local.get('price') != self._quote_targets[0]
                    and not self._gateway.is_rate_limited):
                self._bid_update_count += 1
                if self._bid_update_count == self._UPDATE_INTERVAL:
                    new_order_sz = self.get_order_size(side='Buy')
                    if order_local.get('size') != new_order_sz:
                        order = self.get_bybit_order_cancel_replace(
                            order_link_id=self._bybit_bid_ord_link_id,
                            p_r_price_=str(self._quote_targets[0]),
                            p_r_qty=new_order_sz)
                        self._gateway.prepare_bybit_amend_order(
                            order=order, is_queued=self._is_order_op_queued)
                    else:
                        order = self.get_bybit_order_cancel_replace(
                            order_link_id=self._bybit_bid_ord_link_id,
                            p_r_price_=str(self._quote_targets[0]))
                        self._gateway.prepare_bybit_amend_order(
                            order=order, is_queued=self._is_order_op_queued)
                    self._bid_update_count = 0
        else:
            self.place_new_bybit_order(side='Buy')
        if self._bybit_ask_ord_link_id is not None:
            order_local = self._bybit_active_orders.get(
                self._bybit_ask_ord_link_id)
            if (order_local is not None and not self._is_order_op_queued[0]
                    and order_local.get('price') != self._quote_targets[1]
                    and not self._gateway.is_rate_limited):
                self._ask_update_count += 1
                if self._ask_update_count == self._UPDATE_INTERVAL:
                    new_order_sz = self.get_order_size(side='Sell')
                    if order_local.get('size') != new_order_sz:
                        order = self.get_bybit_order_cancel_replace(
                            order_link_id=self._bybit_ask_ord_link_id,
                            p_r_price_=str(self._quote_targets[1]),
                            p_r_qty=new_order_sz)
                        self._gateway.prepare_bybit_amend_order(
                            order=order, is_queued=self._is_order_op_queued)
                    else:
                        order = self.get_bybit_order_cancel_replace(
                            order_link_id=self._bybit_ask_ord_link_id,
                            p_r_price_=str(self._quote_targets[1]))
                        self._gateway.prepare_bybit_amend_order(
                            order=order, is_queued=self._is_order_op_queued)
                    self._ask_update_count = 0
        else:
            self.place_new_bybit_order(side='Sell')
