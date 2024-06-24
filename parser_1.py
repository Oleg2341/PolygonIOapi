from datetime import datetime
from sys import stderr
from enum import Enum

from requests import Session
from pandas import DataFrame, to_datetime


class Unit(str, Enum):
    """
    Enum class representing time units for data retrieval.
    """
    DAY = 'day'
    HOUR = 'hour'
    MINUTE = 'minute'


class PolygonAPI:
    """
    Class for interacting with the Polygon.io API to fetch financial market data.
    """
    DOMAIN: str = 'https://api.polygon.io/'
    API_KEY: str = 'apiKey=HLq0CJZsrQubZCXx5g64t23NJprSI8rn'

    def __init__(self):
        """
        Initialize the PolygonAPI class with the name of the class and valid units for data retrieval.
        """
        self.name = self.__class__.__name__.__str__()

        self.__valid_units = ', '.join([unit.value for unit in Unit.__members__.values()])

    def __repr__(self):
        """
        Return a string representation of the class.
        """
        return self.name

    __name__ = __str__ = __repr__

    @staticmethod
    def __datetime_to_str(start: datetime, end: datetime) -> str:
        """
        Convert datetime range to string format required by the API.

        :param start: Start datetime.
        :param end: End datetime.
        :return: String representation of the datetime range.
        """
        return f'{start:%Y-%m-%d}/{end:%Y-%m-%d}'

    @staticmethod
    def __timeframe_to_str(interval: int, unit: str) -> str:
        """
        Convert interval and unit to string format required by the API.

        :param interval: Time interval.
        :param unit: Time unit.
        :return: String representation of the timeframe.
        """
        return f'{interval}/{unit}'

    @staticmethod
    def __params_to_str(params) -> str:
        """
        Convert additional parameters to a query string.

        :param params: Dictionary of additional parameters.
        :return: Query string.
        """
        link = '?'
        for key, value in params.items():
            link += f'{key}={value}&'
        return link

    @staticmethod
    def __resp_to_df(resp) -> DataFrame:
        """
        Convert API response to a pandas DataFrame.

        :param resp: API response.
        :return: DataFrame containing the response data.
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
        Construct the API link for fetching available symbols.

        :return: URL string for symbols.
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
        Construct the API link for fetching candlestick data.

        :param symbol: Stock symbol.
        :param tf: Timeframe string.
        :param time: Time range string.
        :param kwargs: Additional parameters.
        :return: URL string for candlestick data.
        
        Example:
        >>> api = PolygonAPI()
        >>> api.get_candles_link('AAPL', '1/day', '2023-01-01/2023-01-31', adjusted='true')
        'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-01-01/2023-01-31?adjusted=true&apiKey=HLq0CJZsrQubZCXx5g64t23NJprSI8rn'
        """
        return self.DOMAIN + f'v2/aggs/ticker/{symbol}/range/{tf}/{time}' + \
            self.__params_to_str(kwargs) + self.API_KEY

    def get_symbols(self) -> list:
        """
        Fetch the list of available symbols from the API.

        :return: List of symbols.
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
        Fetch candlestick data for a given symbol and timeframe.

        :param symbol: Stock symbol.
        :param interval: Time interval.
        :param unit: Time unit (e.g., 'day', 'hour', 'minute').
        :param start: Start datetime.
        :param end: End datetime.
        :param limit: Maximum number of records to fetch.
        :param kwargs: Additional parameters.
        :return: DataFrame containing candlestick data or error message.
        
        Example:
        >>> api = PolygonAPI()
        >>> start = datetime(2023, 1, 1)
        >>> end = datetime(2023, 1, 31)
        >>> df = api.get_klines('AAPL', 1, 'day', start, end, adjusted='true')
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
