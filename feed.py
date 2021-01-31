from typing import Union, Tuple
from abc import abstractmethod
from order_book import BybitOrderBook, BinanceOrderBook
import strategy


class Feed:
    _last_bbo: Tuple[float, float]
    _strategy: strategy.Strategy

    def __init__(self, strat: strategy.Strategy) -> None:
        self._strategy = strat

    @abstractmethod
    def on_websocket(self, data: dict) -> None:
        pass

    @abstractmethod
    def on_order_snapshot(self, data: dict) -> None:
        pass

    @abstractmethod
    def on_position_snapshot(self, data: dict) -> None:
        pass


class BybitFeed(Feed):
    _order_book: BybitOrderBook

    def __init__(self, strat: strategy.Strategy) -> None:
        super().__init__(strat=strat)

    def on_websocket(self, data: dict) -> None:
        if data.get('topic') == 'orderBookL2_25.BTCUSD':
            self.handle_order_book_l2(data=data)
        elif data.get('topic') == 'order':
            self._strategy.on_bybit_order_update(data=data)
        elif data.get('topic') == 'execution':
            self._strategy.on_bybit_execution(data=data)

    def handle_order_book_l2(self, data: dict) -> None:
        if data.get('type') == 'snapshot':
            self._order_book = BybitOrderBook(
                depth_snapshot=data)
            curr_bbo = (self._order_book.bids[0][0],
                        self._order_book.asks[0][0])
            self._strategy.on_bybit_bbo_chg(data=curr_bbo)
            self._last_bbo = curr_bbo
        else:
            self._order_book.handle_delta(delta_message=data)
            curr_bbo = (self._order_book.bids[0][0],
                        self._order_book.asks[0][0])
            if curr_bbo != self._last_bbo:
                self._strategy.on_bybit_bbo_chg(data=curr_bbo)
            self._last_bbo = curr_bbo

    def on_order_snapshot(self, data: dict) -> None:
        self._strategy.on_bybit_order_snap(data=data)

    def on_position_snapshot(self, data: dict) -> None:
        self._strategy.on_bybit_position_snap(data=data)


class BinanceFeed(Feed):
    _buf_depth_updates = []
    _order_book: Union[None, BinanceOrderBook] = None

    def __init__(self, strat: strategy.Strategy) -> None:
        super().__init__(strat=strat)

    def on_websocket(self, data: dict) -> None:
        if data.get('e') == 'depthUpdate':
            self.handle_book_delta(data=data)
        elif data.get('e') == 'ACCOUNT_UPDATE':
            pass
            #self._strategy.on_binance_account_update(data=data.get('a'))
        elif data.get('e') == 'ORDER_TRADE_UPDATE':
            pass
            #self._strategy.on_binance_order_trade_update(data=data.get('o'))

    def on_order_snapshot(self, data: dict) -> None:
        pass
        #self._strategy.on_binance_order_snap(data=data)

    def on_position_snapshot(self, data: dict) -> None:
        for pos in data:
            if pos.get('symbol') == 'BTCUSD_PERP':
                break

    def on_depth_snapshot(self, data: dict) -> None:
        self.handle_book_snapshot(data=data)

    def on_book_reset(self) -> None:
        self._order_book = None

    def handle_book_delta(self, data: dict) -> None:
        if self._order_book is None:
            self._buf_depth_updates.append(data)
        else:
            self._order_book.parse_update(depth_update=data)
            curr_bbo = (self._order_book.bids[0][0],
                        self._order_book.asks[0][0])
            if curr_bbo != self._last_bbo:
                self._strategy.on_binance_bbo_chg(data=curr_bbo)
            self._last_bbo = curr_bbo

    def handle_book_snapshot(self, data: dict) -> None:
        self._order_book = BinanceOrderBook(depth_snapshot=data)
        self.remove_prior_depth_updates(depth_snapshot=data)
        for update in self._buf_depth_updates:
            self._order_book.parse_update(depth_update=update)
        self._buf_depth_updates.clear()
        curr_bbo = (self._order_book.bids[0][0],
                    self._order_book.asks[0][0])
        self._strategy.on_binance_bbo_chg(data=curr_bbo)
        self._last_bbo = curr_bbo

    def remove_prior_depth_updates(self, depth_snapshot: dict):
        removed_updates = []
        for update in self._buf_depth_updates:
            if update.get('u') < depth_snapshot.get('lastUpdateId'):
                removed_updates.append(update)
        for update in removed_updates:
            self._buf_depth_updates.remove(update)
