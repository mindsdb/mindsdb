import pandas as pd
import scipy as sp
from pydantic import BaseModel
from collections import namedtuple
from enum import Enum


def item_mapping(
		item_df: pd.DataFrame,
		item_id_column_name,
		item_description_column_name
) -> dict:
	"""

	takes in item metadata and creates a dict with key being mapped against the index and values being
	a namedtuple containing item id and product name. Creates an easy way to see what was predicted to a given user

	:param item_df:
	:param item_id_column_name:
	:param item_description_column_name:


	:return dict:
	"""
	item_map = {}
	item_data = namedtuple("ItemData", [item_id_column_name, item_description_column_name])

	for idx, item in enumerate(
			zip(
				item_df[item_id_column_name],
				item_df[item_description_column_name]
			)
	):
		item_map[idx] = item_data._make(item)

	return item_map


class RecommenderType(Enum):
	cf = 1
	hybrid = 2


class RecommenderPreprocessorOutput(BaseModel):
	interaction_df: pd.DataFrame
	interaction_matrix: sp.sparse.coo_matrix

	class Config:
		arbitrary_types_allowed = True


class RecommenderPreprocessor:

	def __init__(
			self,
			interaction_data: pd.DataFrame,
			user_id_column_name: str,
			item_id_column_name: str,
			threshold: int = 4,
			recommender_type=RecommenderType.cf
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

		return (self.interaction_data[self.user_id_column_name].max(), self.interaction_data[self.item_id_column_name].max())

	def prevent_cold_start(self):
		"""
		get unique products and items, map to df ids in order to reduce sparcity and prevent cold start
		in collaborative filtering

		:return void:
		"""

		unique_user_ids = {v:k for k,v in enumerate(self.interaction_data[self.user_id_column_name].unique(), 1)}
		unique_item_ids = {v:k for k,v in enumerate(self.interaction_data[self.item_id_column_name].unique(), 1)}

		self.interaction_data['dense_user_id'] = self.interaction_data[self.user_id_column_name].map(unique_user_ids)
		self.interaction_data['dense_item_id'] = self.interaction_data[self.item_id_column_name].map(unique_item_ids)

	def encode_interactions(self):
		"""
		set whether user interacted positively or negatively with item,
		negative may not be applicable depending on the use case

		:return void:
		"""
		# positive interaction
		self.interaction_data.loc[self.interaction_data.rating >= self.threshold, "interaction"] = 1
		# negative interaction
		self.interaction_data.loc[self.interaction_data.rating <= self.threshold, "interaction"] = -1

		if self.recommender_type == 'cf':
			self.prevent_cold_start()

	def construct_interaction_matrix(self) -> sp.sparse.coo_matrix:
		"""
		construct user x item interaction matrix

		:return sp.sparse.coo_matrix :
		"""

		if self.recommender_type.name == "cf":
			self.prevent_cold_start()

			# update id cols to be used for sparse matrix value assignment
			setattr(self, 'user_id_column_name', 'dense_user_id')
			setattr(self, 'item_id_column_name', 'dense_item_id')

		lil_matrix = sp.sparse.lil_matrix(self.n_users_items)

		for index, series in self.interaction_data[[self.user_id_column_name, self.item_id_column_name, 'interaction']].iterrows():
			lil_matrix[int(series[self.user_id_column_name] - 1), int(series[self.item_id_column_name] - 1)] = series[
				'interaction']

		# convert from lil_matrix to coo_matrix

		return lil_matrix.tocoo()

	def preprocess(self) -> RecommenderPreprocessorOutput:
		"""
		runs a series of preprocessing tasks for recommender

		:return RecommenderPreprocessorOutput:
		"""
		self.encode_interactions()
		interaction_matrix = self.construct_interaction_matrix()

		return RecommenderPreprocessorOutput(
			interaction_df=self.interaction_data,
			interaction_matrix=interaction_matrix,
		)


