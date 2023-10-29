import time

from mindsdb.integrations.libs.api_handler import APITable


class KrakenTradesTable(APITable):
    def select_trade_history(self, queries: dict):
        """Selects data from the Kraken API and returns it as a pandas DataFrame.
        Returns dataframe representing the Kraken API results.
        Args:
            queries in dict key-value pair
        """
        # nounce - default mandatory field
        data = {"nounce": str(int(1000 * time.time()))}

        supporting_filters = [
            "nonce",
            "type",
            "trades",
            "start",
            "end",
            "ofs",
            "consolidate_taker",
        ]
        for query, val in queries:
            if query in supporting_filters:
                data[query] = val

        trade_history_data = self.handler.call_kraken_api(
            method_name="get_trade_history", params=data
        )

        return trade_history_data

    def get_columns(self):
        """Gets all columns to be returned in pandas DataFrame responses"""
        return [
            "ordertxid",
            "postxid",
            "pair",
            "time",
            "type",
            "ordertype",
            "price",
            "cost",
            "fee",
            "vol",
            "margin",
            "misc",
            "leverage",
        ]
