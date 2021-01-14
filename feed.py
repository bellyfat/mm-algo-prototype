import multiprocessing.connection
import multiprocessing as mp
from typing import Dict, Union
from abc import abstractmethod
from order_book import BybitOrderBook, BinanceOrderBook


class Feed:
    _pipe: Dict[str, mp.connection.Connection]

    def __init__(self, pipe: Dict[str, mp.connection.Connection]) -> None:
        self._pipe = pipe

    @abstractmethod
    def process_feed(self) -> None:
        pass


class BybitFeed(Feed):
    order_book: BybitOrderBook

    def __init__(self, pipe: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(pipe=pipe)

    def process_feed(self) -> None:
        conn_ws = self._pipe.get('websocket_stream')
        if conn_ws.poll():
            data = conn_ws.recv()
            if data.get('topic') == 'orderBookL2_25.BTCUSD':
                if data.get('type') == 'snapshot':
                    self.order_book = BybitOrderBook(depth_snapshot=data)
                else:
                    self.order_book.handle_delta(delta_message=data)
                print('Bybit Best Bid:', str(self.order_book.bids[0][0])
                      + '; Bybit Best Ask:', str(self.order_book.asks[0][0]))


class BinanceFeed(Feed):
    _buf_depth_updates = []
    order_book: Union[None, BinanceOrderBook]

    def __init__(self, pipe: Dict[str, mp.connection.Connection]) -> None:
        self.order_book = None
        super().__init__(pipe=pipe)

    def process_feed(self) -> None:
        conn_ws = self._pipe.get('websocket_stream')
        if conn_ws.poll():
            data = conn_ws.recv()
            if data.get('e') == 'depthUpdate':
                if self.order_book is None:
                    self._buf_depth_updates.append(data)
                else:
                    self.order_book.parse_update(depth_update=data)
                    print('Binance Best Bid:', str(self.order_book.bids[0][0])
                          + '; Binance Best Ask:',
                          str(self.order_book.asks[0][0]))
        if self.order_book is None:
            conn_depth_snp = self._pipe.get('depth_snapshot')
            if conn_depth_snp.poll():
                depth_snapshot = conn_depth_snp.recv()
                self.order_book = BinanceOrderBook(
                    depth_snapshot=depth_snapshot)
                self.remove_prior_depth_updates(depth_snapshot=depth_snapshot)
                for update in self._buf_depth_updates:
                    self.order_book.parse_update(depth_update=update)
                self._buf_depth_updates.clear()

    def remove_prior_depth_updates(self, depth_snapshot: dict):
        removed_updates = []
        for update in self._buf_depth_updates:
            if update.get('u') < depth_snapshot.get('lastUpdateId'):
                removed_updates.append(update)
        for update in removed_updates:
            self._buf_depth_updates.remove(update)
