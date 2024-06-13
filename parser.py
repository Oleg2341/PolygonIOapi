from datetime import datetime
from sys import stderr
from enum import Enum

from requests import Session
from pandas import DataFrame, to_datetime


class Unit(str, Enum):
    DAY = 'day'
    HOUR = 'hour'
    MINUTE = 'minute'


class PolygonAPI:
    DOMAIN: str = 'https://api.polygon.io/'
    API_KEY: str = 'apiKey=HLq0CJZsrQubZCXx5g64t23NJprSI8rn'

    def __init__(self):
        self.name = self.__class__.__name__.__str__()

        self.__valid_units = ', '.join([unit.value for unit in Unit.__members__.values()])

    def __repr__(self):
        return self.name

    __name__ = __str__ = __repr__

    @staticmethod
    def __datetime_to_str(start: datetime, end: datetime) -> (str, str):
        """

        :param start:
        :param end:
        :return:
        """

        return f'{start:%Y-%m-%d}/{end:%Y-%m-%d}'

    @staticmethod
    def __timeframe_to_str(interval: int, unit: str) -> (str, str):
        """

        :param interval:
        :param unit:
        :return:
        """

        return f'{interval}/{unit}'

    @staticmethod
    def __params_to_str(params) -> str:
        """

        :param params:
        :return:
        """

        link = '?'
        for key, value in params.items():
            link += f'{key}={value}&'
        return link

    @staticmethod
    def __resp_to_df(resp) -> DataFrame:
        """

        :param resp:
        :return:
        """

        if resp is None:
            return DataFrame()

        df = DataFrame.from_dict(resp)
        df = df[['t', 'o', 'h', 'l', 'c', 'v']]
        df['t'] = to_datetime(df['t'], unit='ms')
        return df.rename(columns={
            't': 'time', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})

    @property
    def symbols_link(self) -> str:
        """

        :return:
        """

        return self.DOMAIN + 'v3/reference/tickers?' + self.API_KEY

    def get_candles_link(
            self,
            symbol: str,
            tf: str,
            time: str,
            **kwargs: str
    ) -> str:
        """

        :return:
        """

        return self.DOMAIN + f'v2/aggs/ticker/{symbol}/range/{tf}/{time}' + \
            self.__params_to_str(kwargs) + self.API_KEY

    def get_symbols(self) -> list:
        """

        :return:
        """

        with Session() as s:
            return s.get(self.symbols_link).json().get('results')

    def get_klines(
            self,
            symbol: str,
            interval: int,
            unit: Unit | str,
            start: datetime,
            end: datetime,
            limit: int = 50000,
            **kwargs: str
    ) -> None | DataFrame | dict:
        """

        :param symbol:
        :param interval:
        :param unit:
        :param start:
        :param end:
        :param limit:
        :return:
        """

        try:
            unit = Unit(unit)
        except ValueError:
            return print(f'Invalid unit. Use {Unit} or one of: {self.__valid_units}', file=stderr)

        with Session() as s:
            results = s.get(self.get_candles_link(
                symbol=symbol,
                tf=self.__timeframe_to_str(interval, unit),
                time=self.__datetime_to_str(start, end),
                limit=str(limit),
                **kwargs
            )).json().get('results')

        return self.__resp_to_df(results)
