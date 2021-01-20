import multiprocessing.connection
import multiprocessing as mp
from typing import Dict, Union, List, Tuple
from abc import abstractmethod
from order_book import BybitOrderBook, BinanceOrderBook


class Feed:
    _ws_conns: Dict[str, mp.connection.Connection]
    _strategy_conns: Dict[str, mp.connection.Connection]

    _last_bbo_: Tuple[float, float]

    def __init__(self, ws_conns: Dict[str, mp.connection.Connection],
                 strategy_conns: Dict[str, mp.connection.Connection]) -> None:
        self._ws_conns = ws_conns
        self._strategy_conns = strategy_conns

    @abstractmethod
    def process_feed(self) -> None:
        pass


class BybitFeed(Feed):
    _order_book: BybitOrderBook

    def __init__(self, ws_conns: Dict[str, mp.connection.Connection],
                 strategy_conns: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(ws_conns=ws_conns, strategy_conns=strategy_conns)

    def process_feed(self) -> None:
        for key, conn in self._ws_conns.items():
            if conn.poll():
                data = conn.recv()
                if key == 'websocket_stream':
                    if data.get('topic') == 'orderBookL2_25.BTCUSD':
                        if data.get('type') == 'snapshot':
                            self._order_book = BybitOrderBook(
                                depth_snapshot=data)
                            curr_bbo = (self._order_book.bids[0][0],
                                        self._order_book.asks[0][0])
                            self._strategy_conns.get('bybit_bbo_chg').send(
                                obj=curr_bbo)
                            self._last_bbo_ = curr_bbo
                        else:
                            self._order_book.handle_delta(delta_message=data)
                            curr_bbo = (self._order_book.bids[0][0],
                                        self._order_book.asks[0][0])
                            if curr_bbo != self._last_bbo_:
                                self._strategy_conns.get('bybit_bbo_chg').send(
                                    obj=curr_bbo)
                            self._last_bbo_ = curr_bbo
                    elif data.get('topic') == 'order':
                        self._strategy_conns.get('bybit_order').send(obj=data)
                    elif data.get('topic') == 'execution':
                        self._strategy_conns.get('bybit_execution').send(obj=data)
                elif key == 'active_orders':
                    self._strategy_conns.get('bybit_active_orders').send(
                        obj=data)
                elif key == 'positions':
                    self._strategy_conns.get('bybit_position').send(
                        obj=data)


class BinanceFeed(Feed):
    _buf_depth_updates = []
    _order_book: Union[None, BinanceOrderBook]

    def __init__(self, ws_conns: Dict[str, mp.connection.Connection],
                 strategy_conns: Dict[str, mp.connection.Connection]) -> None:
        self._order_book = None
        super().__init__(ws_conns=ws_conns, strategy_conns=strategy_conns)

    def process_feed(self) -> None:
        for key, conn in self._ws_conns.items():
            if conn.poll():
                data = conn.recv()
                if key == 'book_reset':
                    self._order_book = None
                elif key == 'websocket_stream':
                    if data.get('e') == 'depthUpdate':
                        if self._order_book is None:
                            self._buf_depth_updates.append(data)
                        else:
                            self._order_book.parse_update(depth_update=data)
                            curr_bbo = (self._order_book.bids[0][0],
                                        self._order_book.asks[0][0])
                            if curr_bbo != self._last_bbo_:
                                self._strategy_conns.get(
                                    'binance_bbo_chg').send(
                                    obj=curr_bbo)
                            self._last_bbo_ = curr_bbo
                    elif data.get('e') == 'ORDER_TRADE_UPDATE':
                        self._strategy_conns.get(
                            'binance_order_trade_upd').send(obj=data.get('o'))
                elif key == 'depth_snapshot':
                    self._order_book = BinanceOrderBook(depth_snapshot=data)
                    self.remove_prior_depth_updates(depth_snapshot=data)
                    for update in self._buf_depth_updates:
                        self._order_book.parse_update(depth_update=update)
                    self._buf_depth_updates.clear()
                    curr_bbo = (self._order_book.bids[0][0],
                                self._order_book.asks[0][0])
                    self._strategy_conns.get('binance_bbo_chg').send(
                        obj=curr_bbo)
                    self._last_bbo_ = curr_bbo
                elif key == 'open_orders':
                    self._strategy_conns.get('binance_open_orders').send(
                        obj=data)
                elif key == 'positions':
                    for pos in data:
                        if pos.get('symbol') == 'BTCUSD_PERP':
                            self._strategy_conns.get('binance_position').send(
                                obj=pos)
                            break

    def remove_prior_depth_updates(self, depth_snapshot: dict):
        removed_updates = []
        for update in self._buf_depth_updates:
            if update.get('u') < depth_snapshot.get('lastUpdateId'):
                removed_updates.append(update)
        for update in removed_updates:
            self._buf_depth_updates.remove(update)
