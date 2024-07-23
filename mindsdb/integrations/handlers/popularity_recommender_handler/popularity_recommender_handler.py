from typing import Dict, Optional

import dill
import pandas as pd
import polars as pl

from mindsdb.integrations.libs.base import BaseMLEngine


class PopularityRecommenderHandler(BaseMLEngine):
    """
    Integration with polar based popularity recommender.
    """

    name = "popularity-recommender"

    def create(
        self,
        target: str,
        df: pd.DataFrame = None,
        meta_data: Dict = None,
        args: Optional[Dict] = None,
    ) -> None:

        args = args["using"]

        interaction_data = pl.from_pandas(df)

        args["ave_per_item_user"] = (
            interaction_data.get_column(args["user_id"])
            .value_counts()
            .mean()["count"][0]
        )

        popularity = (
            interaction_data.get_column(args["item_id"])
            .value_counts()
            .sort("count", descending=True)
            .get_column(args["item_id"])
            .head(
                int(args["n_recommendations"] * args["ave_per_item_user"])
            )  # to ensure there are enough to predict
            .to_pandas()
            .reset_index()
            .to_dict(orient="list")
        )

        self.model_storage.file_set("interaction", dill.dumps(df))
        self.model_storage.json_set("popularity", popularity)
        self.model_storage.json_set("args", args)

    def predict(self, df=None, args: Optional[dict] = None):

        args = self.model_storage.json_get("args")
        popularity = self.model_storage.json_get("popularity")
        interaction = dill.loads(self.model_storage.file_get("interaction"))

        global_popularity = [*popularity.values()][1]

        if df is not None:
            # get recommendations for specific users if specified
            user_ids = df[args["user_id"]].unique().tolist()

            interaction_data = pl.from_pandas(interaction).filter(
                pl.col(args["user_id"]).is_in(user_ids)
            )

        else:
            # get recommendations for all users
            interaction_data = pl.from_pandas(interaction)

        # aggregate over user
        interacted_items = (
            interaction_data.groupby(args["user_id"])
            .agg(pl.col(args["item_id"]).alias("items"))
            .with_columns(
                pl.lit([global_popularity]).alias("popular_items"),
            )
        ).lazy()

        df = (
            (
                interacted_items.join(
                    (
                        interacted_items.explode("popular_items")
                        .filter(pl.col("popular_items").is_in("items").is_not())
                        .groupby(args["user_id"])
                        .agg(recommended="popular_items")
                    ),
                    on=args["user_id"],
                ).select(
                    [
                        pl.col(args["user_id"]),
                        pl.col("recommended").list.head(args["n_recommendations"]),
                    ]
                )
            )
            .explode("recommended")
            .collect()
            .to_pandas()
        )

        return df
