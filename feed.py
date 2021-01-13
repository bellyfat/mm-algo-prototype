import multiprocessing.connection
import multiprocessing as mp
from typing import Dict
from abc import abstractmethod


class Feed:
    _pipe: Dict[str, mp.connection.Connection]

    def __init__(self, pipe: Dict[str, mp.connection.Connection]) -> None:
        self._pipe = pipe

    @abstractmethod
    def process_feed(self) -> None:
        pass


class BybitFeed(Feed):
    def __init__(self, pipe: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(pipe=pipe)

    def process_feed(self) -> None:
        conn_ws = self._pipe.get('websocket_stream')
        if conn_ws.poll():
            data = conn_ws.recv()
            print(data)


class BinanceFeed(Feed):
    _buf_depth_snapshot = None
    _buf_depth_updates = []

    def __init__(self, pipe: Dict[str, mp.connection.Connection]) -> None:
        super().__init__(pipe=pipe)

    def process_feed(self) -> None:
        conn_ws = self._pipe.get('websocket_stream')
        if conn_ws.poll():
            data = conn_ws.recv()
            print(data)
            if data.get('e') == 'depthUpdate':
                self._buf_depth_updates.append(data)
        if self._buf_depth_snapshot is None:
            conn_depth_snp = self._pipe.get('depth_snapshot')
            if conn_depth_snp.poll():
                self._buf_depth_snapshot = conn_depth_snp.recv()
                print(self._buf_depth_snapshot)
                print(len(self._buf_depth_updates))

