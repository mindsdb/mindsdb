import sib_api_v3_sdk
import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

from mindsdb_sql.parser import ast

from mindsdb.utilities import log


class EmailCampaignsTable(APITable):
    """The Sendinblue Email Campaigns Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Sendinblue "GET /emailCampaigns" API endpoint.

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            Sendinblue Email Campaigns matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 20

        email_campaigns_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "":
                    next
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        configuration = self.handler.connect()
        email_campaigns_api_instance = sib_api_v3_sdk.EmailCampaignsApi(sib_api_v3_sdk.ApiClient(configuration))
        email_campaigns = email_campaigns_api_instance.get_email_campaigns()

        email_campaigns_df = pd.json_normalize(email_campaigns['campaigns'])

        return email_campaigns_df