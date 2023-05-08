from typing import Optional, Dict

import polars as pl
import pandas as pd


from mindsdb.integrations.libs.base import BaseMLEngine


class PopularityRecommenderHandler(BaseMLEngine):
    """
    Integration with the lightfm Recommender library.
    """

    name = 'popularity-recommender'

    def create(self, data: pd.DataFrame = None, meta_data: Dict = None, args: Optional[Dict] = None) -> None:

        #args = args["using"]

        df = pl.from_pandas(data)

        args['ave_per_item_user'] = (df
                             .get_column(args["user_id"])
                             .value_counts()
                             .mean()['counts'][0]
                             )

        popularity = (
            df
            .get_column(args["item_id"])
            .value_counts()
            .sort("counts", descending=True)
            .get_column(args["item_id"])
            .head(args["n_recommendations"]*args['ave_per_item_user']) # to ensure there are enough to predict
            .to_pandas()
            .reset_index()
            .to_dict(orient='list')
        )

        return popularity, args

        # self.model_storage.json_set('popularity', popularity)
        # self.model_storage.json_set('args', args)


    def predict(self, data=None, args: Optional[dict] = None):


        # args = self.model_storage.json_get('args')
        # popularity = self.model_storage.json_get('popularity')

        global_popularity = (
            pl.from_dict(args['global_popularity'])
            .get_column(args['item_id'])
            .to_list()
        )

        interaction_data = pl.from_pandas(data)

        # aggregate over user
        interacted_items = (
            interaction_data
            .groupby(args["user_id"])
            .agg(pl.col(args['item_id']).alias('items'))
            .with_columns(
                pl.lit([global_popularity]).alias("popular_items"),
            )
        ).lazy()

        df = interacted_items.join(
            (interacted_items.explode("popular_items")
             .filter(pl.col("popular_items").is_in("items").is_not())
             .groupby("customer_id")
             .agg(suggested="popular_items")),
            on="customer_id"
        ).collect().to_pandas()

        return df


if __name__ == '__main__':

    _data = pd.read_csv("/Users/d/PycharmProjects/mindsdb/mindsdb/integrations/handlers/popularity_rec_handler/transactions.csv")

    _args = {
        "item_id": "article_id",
        "user_id": "customer_id",
        "n_recommendations": 2
    }


    pop_rec = PopularityRecommenderHandler("f","f")
    pop,args = pop_rec.create(data=_data, args=_args)
    args['global_popularity'] = pop
    df = pop_rec.predict(data=_data,args=args)
    print('5')



