from typing import List, Union


class OrderBook:
    bids: List[list]
    asks: List[list]
    BID_STR = 'BID'
    ASK_STR = 'ASK'

    def __init__(self, bids: List[list], asks: List[list]) -> None:
        self.bids = bids
        self.asks = asks

    def delete(self, side: str, level: List[Union[float, int]]) -> None:
        remove_level = None
        if side == self.BID_STR:
            for compare in self.bids:
                if compare[0] == level[0]:
                    remove_level = compare
                    break
            if remove_level is not None:
                self.bids.remove(remove_level)
        else:
            for compare in self.asks:
                if compare[0] == level[0]:
                    remove_level = compare
                    break
            if remove_level is not None:
                self.asks.remove(remove_level)


class BybitOrderBook(OrderBook):
    BID_STR = 'Buy'
    ASK_STR = 'Sell'

    def __init__(self, depth_snapshot: dict) -> None:
        bids = []
        asks = []
        for level in depth_snapshot.get('data'):
            if level.get('side') == self.BID_STR:
                bids.append(self.get_parsed_lvl(level=level))
            else:
                asks.append(self.get_parsed_lvl(level=level))
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        super().__init__(bids=bids, asks=asks)

    @staticmethod
    def get_parsed_lvl(level: dict) -> List[Union[float, int]]:
        return [float(level.get('price')), level.get('size')]

    def update(self, side: str, level: List[Union[float, int]]) -> None:
        if side == self.BID_STR:
            for compare in self.bids:
                if compare[0] == level[0]:
                    compare[1] = level[1]
                    return
        else:
            for compare in self.asks:
                if compare[0] == level[0]:
                    compare[1] = level[1]
                    return

    def insert(self, side: str, level: List[Union[float, int]]) -> None:
        if side == self.BID_STR:
            self.bids.append(level)
            self.bids.sort(key=lambda x: x[0], reverse=True)
        else:
            self.asks.append(level)
            self.asks.sort(key=lambda x: x[0])

    def handle_delta(self, delta_message: dict) -> None:
        data = delta_message.get('data')
        for level in data.get('delete'):
            self.delete(side=level.get('side'),
                        level=self.get_parsed_lvl(level=level))
        for level in data.get('update'):
            self.update(side=level.get('side'),
                        level=self.get_parsed_lvl(level=level))
        for level in data.get('insert'):
            self.insert(side=level.get('side'),
                        level=self.get_parsed_lvl(level=level))


class BinanceOrderBook(OrderBook):
    def __init__(self, depth_snapshot: dict) -> None:
        super().__init__(
            bids=self.convert_response_list(values=depth_snapshot.get('bids')),
            asks=self.convert_response_list(values=depth_snapshot.get('asks')))

    @staticmethod
    def convert_response_list(values: List[List[str]]) -> List[list]:
        converted = []
        for value in values:
            converted.append([float(value[0]), int(value[1])])
        return converted

    def insert_or_update(self, side: str, level: List[list]) -> None:
        if side == self.BID_STR:
            for compare in self.bids:
                if compare[0] == level[0]:
                    compare[1] = level[1]
                    return
            self.bids.append(level)
            self.bids.sort(key=lambda x: x[0], reverse=True)
        else:
            for compare in self.asks:
                if compare[0] == level[0]:
                    compare[1] = level[1]
                    return
            self.asks.append(level)
            self.asks.sort(key=lambda x: x[0])

    def parse_update(self, depth_update: dict) -> None:
        raw_bids = depth_update.get('b')
        if raw_bids is not None:
            parsed_bids = self.convert_response_list(values=raw_bids)
            for bid in parsed_bids:
                if bid[1] == 0:
                    self.delete(side=self.BID_STR, level=bid)
                else:
                    self.insert_or_update(side=self.BID_STR, level=bid)
        raw_asks = depth_update.get('a')
        if raw_asks is not None:
            parsed_asks = self.convert_response_list(values=raw_asks)
            for ask in parsed_asks:
                if ask[1] == 0:
                    self.delete(side=self.ASK_STR, level=ask)
                else:
                    self.insert_or_update(side=self.ASK_STR, level=ask)


