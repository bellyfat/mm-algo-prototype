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
    order_book: BybitOrderBook

    def __init__(self, ws_conns: Dict[str, mp.connection.Connection],
                 strategy_conns: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(ws_conns=ws_conns, strategy_conns=strategy_conns)

    def process_feed(self) -> None:
        conn_ws = self._ws_conns.get('websocket_stream')
        if conn_ws.poll():
            data = conn_ws.recv()
            if data.get('topic') == 'orderBookL2_25.BTCUSD':
                if data.get('type') == 'snapshot':
                    self.order_book = BybitOrderBook(depth_snapshot=data)
                    curr_bbo = (self.order_book.bids[0][0],
                                self.order_book.asks[0][0])
                    self._strategy_conns.get('bybit_bbo_chg').send(obj=curr_bbo)
                    self._last_bbo_ = curr_bbo
                else:
                    self.order_book.handle_delta(delta_message=data)
                    curr_bbo = (self.order_book.bids[0][0],
                                self.order_book.asks[0][0])
                    if curr_bbo != self._last_bbo_:
                        self._strategy_conns.get('bybit_bbo_chg').send(
                            obj=curr_bbo)
                    self._last_bbo_ = curr_bbo


class BinanceFeed(Feed):
    _buf_depth_updates = []
    order_book: Union[None, BinanceOrderBook]

    def __init__(self, ws_conns: Dict[str, mp.connection.Connection],
                 strategy_conns: Dict[str, mp.connection.Connection]) -> None:
        self.order_book = None
        super().__init__(ws_conns=ws_conns, strategy_conns=strategy_conns)

    def process_feed(self) -> None:
        conn_ws = self._ws_conns.get('websocket_stream')
        if conn_ws.poll():
            data = conn_ws.recv()
            if data.get('e') == 'depthUpdate':
                if self.order_book is None:
                    self._buf_depth_updates.append(data)
                else:
                    self.order_book.parse_update(depth_update=data)
                    curr_bbo = (self.order_book.bids[0][0],
                                self.order_book.asks[0][0])
                    if curr_bbo != self._last_bbo_:
                        self._strategy_conns.get('binance_bbo_chg').send(
                            obj=curr_bbo)
                    self._last_bbo_ = curr_bbo
        if self.order_book is None:
            conn_depth_snp = self._ws_conns.get('depth_snapshot')
            if conn_depth_snp.poll():
                depth_snapshot = conn_depth_snp.recv()
                self.order_book = BinanceOrderBook(
                    depth_snapshot=depth_snapshot)
                self.remove_prior_depth_updates(depth_snapshot=depth_snapshot)
                for update in self._buf_depth_updates:
                    self.order_book.parse_update(depth_update=update)
                self._buf_depth_updates.clear()
                # Send order book bbo
                curr_bbo = (self.order_book.bids[0][0],
                            self.order_book.asks[0][0])
                self._strategy_conns.get('binance_bbo_chg').send(obj=curr_bbo)
                self._last_bbo_ = curr_bbo

    def remove_prior_depth_updates(self, depth_snapshot: dict):
        removed_updates = []
        for update in self._buf_depth_updates:
            if update.get('u') < depth_snapshot.get('lastUpdateId'):
                removed_updates.append(update)
        for update in removed_updates:
            self._buf_depth_updates.remove(update)
