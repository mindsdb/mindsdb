from enum import Enum

import lightfm
import numpy as np
import pandas as pd
import scipy as sp
from pydantic import BaseModel


def get_user_item_recommendations(
    n_users: int, n_items: int, args: dict, item_ids, user_ids, model: lightfm.LightFM
):
    """
    gets N user-item recommendations for a given model
    :param n_users:
    :param n_items:
    :param args:
    :param item_ids:
    :param user_ids:
    :param model:
    :return:
    """
    # recommend items for each user

    if item_ids or user_ids:
        item_idx = [
            key for key, val in args["idx_to_item_id_map"].items() if val in item_ids
        ]
        user_idx = [
            key for key, val in args["idx_to_user_id_map"].items() if val in user_ids
        ]

        user_idxs = np.repeat(user_idx, n_items)
        item_idxs = np.tile(item_idx, n_users)

    else:
        user_idxs = np.repeat([i for i in range(0, n_users)], n_items)
        item_idxs = np.tile([i for i in range(0, n_items)], n_users)

    scores = model.predict(user_idxs, item_idxs)

    # map scores to user-item pairs, sort by score and return top N recommendations per user
    user_item_recommendations_df = (
        pd.DataFrame({"user_idx": user_idxs, "item_idx": item_idxs, "score": scores})
        .groupby("user_idx")
        .apply(
            lambda x: x.sort_values("score", ascending=False).head(
                args["n_recommendations"]
            )
        )
    )

    # map idxs to item ids and user ids

    user_item_recommendations_df["item_id"] = (
        user_item_recommendations_df["item_idx"]
        .astype("str")
        .map(args["idx_to_item_id_map"])
    )
    user_item_recommendations_df["user_id"] = (
        user_item_recommendations_df["user_idx"]
        .astype("str")
        .map(args["idx_to_user_id_map"])
    )

    return user_item_recommendations_df[["user_id", "item_id", "score"]].astype(
        {"user_id": "str", "item_id": "str"}
    )


def get_item_item_recommendations(
    model: lightfm.LightFM,
    args: dict,
    item_ids: list = None,
    item_features=None,
    n_recommendations: int = 10,
) -> pd.DataFrame:
    """
    gets similar items to a given item index inside user-item interaction matrix
    NB by default it won't use item features,however if item features are provided
    it will use them to get similar items

    :param args:
    :param model:
    :param item_ids:
    :param item_features:
    :param n_recommendations:

    :return:
    """
    # todo make sure its not slow across larger data
    # todo break into smaller functions

    similar_items_dfs = []

    idx_to_item_id_map = args["idx_to_item_id_map"]

    if item_ids:
        # filter out item_ids that are not in the request
        idx_to_item_id_map = {
            key: val
            for key, val in args["idx_to_item_id_map"].items()
            if val in item_ids
        }

    for item_idx, item_id in idx_to_item_id_map.items():
        # ensure item_idx is int
        item_idx = int(item_idx)

        item_biases, item_representations = model.get_item_representations(
            features=item_features
        )

        # Cosine similarity

        scores = item_representations.dot(item_representations[item_idx, :])

        # normalize
        item_norms = np.sqrt((item_representations * item_representations).sum(axis=1))
        scores /= item_norms

        # get the top N items
        best = np.argpartition(scores, -n_recommendations)
        # sort the scores

        rec = sorted(
            zip(best, scores[best] / item_norms[item_idx]), key=lambda x: -x[1]
        )

        intermediate_df = (
            pd.DataFrame(rec, columns=["item_idx", "score"])
            .tail(-1)  # remove the item itself
            .head(n_recommendations)
        )
        intermediate_df["item_id_one"] = item_id
        similar_items_dfs.append(intermediate_df)

    similar_items_df = pd.concat(similar_items_dfs, ignore_index=True)
    similar_items_df["item_id_two"] = (
        similar_items_df["item_idx"].astype("str").map(idx_to_item_id_map)
    )

    return similar_items_df[["item_id_one", "item_id_two", "score"]].astype(
        {"item_id_one": "str", "item_id_two": "str"}
    )


class ModelParameters(BaseModel):
    learning_rate: float = 0.05
    loss: str = "warp"
    epochs: int = 10


class RecommenderType(Enum):
    cf = 1
    hybrid = 2


# todo add support for hybrid recommender
class RecommenderPreprocessorOutput(BaseModel):
    interaction_df: pd.DataFrame
    interaction_matrix: sp.sparse.coo_matrix
    idx_item_map: dict
    idx_user_map: dict

    class Config:
        arbitrary_types_allowed = True


class RecommenderPreprocessor:
    def __init__(
        self,
        interaction_data: pd.DataFrame,
        user_id_column_name: str,
        item_id_column_name: str,
        threshold: int = 4,
        recommender_type=RecommenderType.cf,
    ):
        self.interaction_data = interaction_data
        self.user_id_column_name = user_id_column_name
        self.item_id_column_name = item_id_column_name
        self.threshold = threshold
        self.recommender_type = recommender_type

    @property
    def n_users_items(self):
        """
        get tuple with number of users and items e.g. user-item matrix shape
        :return tuple:
        """

        return (
            self.interaction_data[self.user_id_column_name].nunique(),
            self.interaction_data[self.item_id_column_name].nunique(),
        )

    @property
    def _idx_item_map(self) -> dict:
        """
        maps item idx in matrix to item id
        :return void:
        """

        return (
            self.interaction_data[[self.item_id_column_name, "item_idx"]]
            .drop_duplicates()
            .set_index("item_idx")
            .to_dict()[self.item_id_column_name]
        )

    @property
    def _idx_user_map(self) -> dict:
        """
        maps user idx in matrix to user id
        :return void:
        """

        return (
            self.interaction_data[[self.user_id_column_name, "user_idx"]]
            .drop_duplicates()
            .set_index("user_idx")
            .to_dict()[self.user_id_column_name]
        )

    def map_id_to_idx(self):
        """
        map user and item ids to a range of 0 to n_users and 0 to n_items respectively
        reduces density of user-item matrix
        :return void:
        """

        unique_user_ids = {
            v: k
            for k, v in enumerate(
                self.interaction_data[self.user_id_column_name].unique(), 0
            )
        }
        unique_item_ids = {
            v: k
            for k, v in enumerate(
                self.interaction_data[self.item_id_column_name].unique(), 0
            )
        }

        self.interaction_data["user_idx"] = self.interaction_data[
            self.user_id_column_name
        ].map(unique_user_ids)
        self.interaction_data["item_idx"] = self.interaction_data[
            self.item_id_column_name
        ].map(unique_item_ids)

    def encode_interactions(self):
        """
        set whether user interacted positively or negatively with item,
        negative may not be applicable depending on the use case
        :return void:
        """
        # encode interactions as 1 or -1 for positive and negative respectively
        self.interaction_data["interaction"] = np.where(
            self.interaction_data.rating >= self.threshold, 1, -1
        ).astype("int64")

    def construct_interaction_matrix(self) -> sp.sparse.coo_matrix:

        """
        construct user x item interaction matrix
        :return sp.sparse.coo_matrix :
        """

        lil_matrix = sp.sparse.lil_matrix(self.n_users_items)

        # populate the matrix
        for index, series in self.interaction_data[
            ["user_idx", "item_idx", "interaction"]
        ].iterrows():
            lil_matrix[series["user_idx"], series["item_idx"]] = series["interaction"]

        # convert from lil_matrix to coo_matrix

        return lil_matrix.tocoo()

    # todo allow user to specify whether ratings data is implicit or explicit
    def preprocess(self) -> RecommenderPreprocessorOutput:
        """
        runs a series of preprocessing tasks for recommender
        :return RecommenderPreprocessorOutput:
        """
        # encode interactions
        self.encode_interactions()

        # get the idx of the user and item
        self.map_id_to_idx()

        # construct interaction matrix
        interaction_matrix = self.construct_interaction_matrix()

        return RecommenderPreprocessorOutput(
            interaction_df=self.interaction_data,
            interaction_matrix=interaction_matrix,
            idx_item_map=self._idx_item_map,
            idx_user_map=self._idx_user_map,
        )
