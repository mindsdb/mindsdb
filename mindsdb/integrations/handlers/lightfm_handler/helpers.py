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


def encode_interactions(data: pd.DataFrame, threshold: int = 4) -> pd.DataFrame:
	"""
	set whether user interacted positively or negatively with item,
	negative may not be applicable depending on the use case

	:param data:
	:param threshold:
	:return pd.DataFrame:
	"""
	# positive interaction
	data.loc[data.rating >= threshold, "interaction"] = 1
	# negative interaction
	data.loc[data.rating <= threshold, "interaction"] = -1

	return data


def item_mapping(data, item_id_column_name, item_description_column_name) -> dict:
	"""
	takes in item metadata and creates a dict with key being mapped against the index and values being
	a namedtuple containing item id and product name. Creates an easy way to see what was predicted to a given user

	:param data:
	:param item_id_column_name:
	:param item_description_column_name:

	:return dict:
	"""
	item_map = {}
	item_data = namedtuple("ItemData", [item_id_column_name, item_description_column_name])

	for idx, item in enumerate(
			zip(
				data[item_id_column_name],
				data[item_description_column_name]
			)
	):
		item_map[idx] = item_data._make(item)

	return item_map


def construct_interaction_matrix(data, user_id_column_name,item_id_column_name) -> sp.sparse.coo_matrix:
	"""
	construct user x item interaction matrix

	:return sp.sparse.coo_matrix :
	"""
	number_unique_users = data[user_id_column_name].max()
	number_unique_products = data[item_id_column_name].max()
	matrix_shape = (number_unique_users, number_unique_products)

	lil_matrix = sp.sparse.lil_matrix(matrix_shape)

	for index, series in data[['userId', 'movieId', 'interaction']].iterrows():
		lil_matrix[int(series['userId'] - 1), int(series['movieId'] - 1)] = series['interaction']

	# convert from lil_matrix to coo_matrix
	coo_matrix = lil_matrix.tocoo()

	return coo_matrix


def preprocess(
		data,
		user_id_column_name,
		item_id_column_name,
		item_description_column_name,
		interaction_threshold=4
) -> RecommenderPreprocessorOutput:
	"""

	runs a series of preprocessing tasks for recommender

	:param data:
	:param interaction_threshold:
	:param user_id_column_name:
	:param item_id_column_name:
	:param item_description_column_name:
	:return RecommenderPreprocessorOutput:
	"""
	interactions_encoded_df = encode_interactions(data, interaction_threshold)
	item_map = item_mapping(data, item_id_column_name, item_description_column_name)
	interaction_matrix = construct_interaction_matrix(data, user_id_column_name,item_id_column_name)

	return RecommenderPreprocessorOutput(
		interaction_df=interactions_encoded_df,
		interaction_matrix=interaction_matrix,
		item_mapping=item_map
	)


