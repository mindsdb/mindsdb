from enum import Enum

import pandas as pd
import scipy as sp
from pydantic import BaseModel

import numpy as np


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
