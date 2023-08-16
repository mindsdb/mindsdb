from os import path
from typing import Collection, Dict

import numpy as np
import pandas as pd
import requests

SERVICE_URL = r"https://pypistats.org"
API_BASE_URL = path.join(SERVICE_URL, "api/packages/")


class PyPI:
    def __init__(self, name: str, limit: int = None) -> None:
        """initializer method

        Args:
            name(str): package name
        """
        self.name: str = name
        self.limit = limit
        self.endpoint: str = path.join(API_BASE_URL, name)

    def recent(self, period: str = None) -> pd.DataFrame:
        """recent endpoint

        Args:
            period (str, optional): the desired `day` or `week` or `month` period. Defaults to None.

        Returns:
            pd.DataFrame: pandas dataframe
        """
        endpoint: str = path.join(self.endpoint, "recent")
        params: Dict = {}

        if period:
            params["period"] = period

        payload = requests.get(endpoint, params=params).json()["data"]

        df = self.__to_dataframe(payload, [0])

        return df

    def overall(self, mirrors: bool = None) -> pd.DataFrame:
        """overall endpoint

        Args:
            mirrors (bool, optional): filter by mirrors. Defaults to None.

        Returns:
            pd.DataFrame: pandas dataframe
        """
        endpoint: str = path.join(self.endpoint, "overall")
        params: Dict = {}

        if mirrors is not None:
            params["mirrors"] = str(mirrors).lower()

        payload = requests.get(endpoint, params=params).json()["data"]
        df = self.__to_dataframe(payload, limit=self.limit)

        return df

    def python_major(self, version: str = None) -> pd.DataFrame:
        """python major endpoint

        Args:
            version (str, optional): filter by the major version number. Defaults to None.

        Returns:
            pd.DataFrame: pandas dataframe
        """
        endpoint: str = path.join(self.endpoint, "python_major")
        params: Dict = {}

        if version is not None:
            params["version"] = version

        payload = requests.get(endpoint, params=params).json()["data"]
        df = self.__to_dataframe(payload, limit=self.limit)

        return df

    def python_minor(self, version: str = None) -> pd.DataFrame:
        """python minor endpoint

        Args:
            version (str, optional): filter by the minor.patch version number. Defaults to None.

        Returns:
            pd.DataFrame: pandas dataframe
        """
        endpoint: str = path.join(self.endpoint, "python_minor")
        params: Dict = {}

        if version is not None:
            params["version"] = version

        payload = requests.get(endpoint, params=params).json()["data"]
        df = self.__to_dataframe(payload, limit=self.limit)

        return df

    def system(self, os: str = None) -> pd.DataFrame:
        """system endpoint

        Args:
            os (str, optional): filter by the operating system. Defaults to None.

        Returns:
            pd.DataFrame: pandas dataframe
        """
        endpoint: str = path.join(self.endpoint, "system")
        params: Dict = {}

        if os is not None:
            params["os"] = os

        payload = requests.get(endpoint, params=params).json()["data"]
        df = self.__to_dataframe(payload, limit=self.limit)

        return df

    @staticmethod
    def __to_dataframe(
        json_data: Dict,
        index: Collection = None,
        limit: int = 20,
    ) -> pd.DataFrame:
        """_summary_

        Args:
            json_data (Dict): data
            index (Collection, optional): desired index. Defaults to None.
            limit (int, optional): limit the output coming from dataframe. Defaults to 20.

        Returns:
            pd.DataFrame: _description_
        """
        df = pd.DataFrame(json_data, index=index)
        df.replace("null", np.nan, inplace=True)
        df = df.dropna()

        return df.tail(limit)

    @classmethod
    def is_connected(cls) -> Dict:
        try:
            _ = requests.get(SERVICE_URL, timeout=5).raise_for_status()
            return {"status": True}
        except requests.exceptions.RequestException as e:
            return {"status": False, "message": e}
