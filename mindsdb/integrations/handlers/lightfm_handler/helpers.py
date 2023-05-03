import pandas as pd
import scipy as sp
from pydantic import BaseModel
from collections import namedtuple


class RecommenderPreprocessorOutput(BaseModel):
	interaction_df: pd.DataFrame
	interaction_matrix: sp.sparse.coo_matrix
	item_mapping: dict

	class Config:
		arbitrary_types_allowed = True


class RecommenderPreprocessor:

	def __init__(
			self,
			interaction_data: pd.DataFrame,
			user_id_column_name: str,
			item_id_column_name: str,
			item_description_column_name: str,
			threshold: int = 4,
			recommender_type: str = "cf"
	):
		# todo use enum for recommender_type
		self.data = interaction_data
		self.user_id_column_name = user_id_column_name
		self.item_id_column_name = item_id_column_name
		self.item_description_column_name = item_description_column_name
		self.threshold = threshold
		self.recommender_type = recommender_type

	def prevent_cold_start(self):
		"""
		get unique products and items, map to df ids in order to reduce sparcity and prevent cold start
		in collaborative filtering

		:return void:
		"""

		unique_user_ids = {v:k for k,v in enumerate(self.data[self.user_id_column_name].unique(), 1)}
		unique_item_ids = {v:k for k,v in enumerate(self.data[self.item_id_column_name].unique(), 1)}

		self.data['dense_user_id'] = self.data[self.user_id_column_name].map(unique_user_ids)
		self.data['dense_item_id'] = self.data[self.item_id_column_name].map(unique_item_ids)

	def encode_interactions(self):
		"""
		set whether user interacted positively or negatively with item,
		negative may not be applicable depending on the use case

		:return void:
		"""
		# positive interaction
		self.data.loc[self.data.rating >= self.threshold, "interaction"] = 1
		# negative interaction
		self.data.loc[self.data.rating <= self.threshold, "interaction"] = -1

		if self.recommender_type == 'cf':
			self.prevent_cold_start()

	def item_mapping(self) -> dict:
		"""
		takes in item metadata and creates a dict with key being mapped against the index and values being
		a namedtuple containing item id and product name. Creates an easy way to see what was predicted to a given user

		:return dict:
		"""
		item_map = {}
		item_data = namedtuple("ItemData", [self.item_id_column_name, self])

		for idx, item in enumerate(
				zip(
					self.data[self.item_id_column_name],
					self.data[self]
				)
		):
			item_map[idx] = item_data._make(item)

		return item_map

	def construct_interaction_matrix(self) -> sp.sparse.coo_matrix:
		"""
		construct user x item interaction matrix

		:return sp.sparse.coo_matrix :
		"""

		if self.recommender_type == "cf":
			self.prevent_cold_start()

			# update id cols to be used for sparse matrix value assignment
			setattr(self, 'user_id_column_name', 'dense_user_id')
			setattr(self, 'item_id_column_name', 'dense_item_id')

		elif self.recommender_type != "hybrid":

			raise (ValueError("recommender_type must be either 'cf' or 'hybrid'"))

		n_users = self.data[self.user_id_column_name].max()
		n_items = self.data[self.item_id_column_name].max()

		matrix_shape = (n_users, n_items)

		lil_matrix = sp.sparse.lil_matrix(matrix_shape)

		for index, series in self.data[[self.user_id_column_name, self.item_id_column_name, 'interaction']].iterrows():
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
		item_map = self.item_mapping()
		interaction_matrix = self.construct_interaction_matrix()

		return RecommenderPreprocessorOutput(
			interaction_df=self.data,
			interaction_matrix=interaction_matrix,
			item_mapping=item_map
		)
