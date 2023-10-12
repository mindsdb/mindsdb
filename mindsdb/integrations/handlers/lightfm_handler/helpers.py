import lightfm
import numpy as np
import pandas as pd


def get_item_user_idx(args: dict, n_users, n_items, item_ids=None, user_ids=None):
    """
    gets item and user idxs from item_ids and user_ids
    :param args:
    :param n_users:
    :param n_items:
    :param item_ids:
    :param user_ids:
    :return:
    """
    if item_ids and user_ids:
        item_idx = np.array(
            [int(args["item_id_to_idx_map"][item_id]) for item_id in item_ids]
        )
        user_idx = np.array(
            [int(args["user_id_to_idx_map"][user_id]) for user_id in user_ids]
        )

        # repeat each user id index n_items times
        user_idxs = np.repeat(user_idx, n_items)

        # repeat the full list of item indexes n_user times
        item_idxs = np.tile(item_idx, n_users)

    elif item_ids and not user_ids:
        item_idx = np.array(
            [int(args["item_id_to_idx_map"][item_id]) for item_id in item_ids]
        )

        user_idxs = np.repeat([i for i in range(0, n_users)], n_items)
        item_idxs = np.tile(item_idx, n_users)

    elif user_ids and not item_ids:
        user_idx = np.array(
            [int(args["user_id_to_idx_map"][user_id]) for user_id in user_ids]
        )

        user_idxs = np.repeat(user_idx, n_items)
        item_idxs = np.tile([i for i in range(0, n_items)], n_users)

    else:
        user_idxs = np.repeat([i for i in range(0, n_users)], n_items)
        item_idxs = np.tile([i for i in range(0, n_items)], n_users)

    return item_idxs, user_idxs


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
    # get idxs for user-item pairs
    item_idxs, user_idxs = get_item_user_idx(args, n_users, n_items, item_ids, user_ids)

    scores = model.predict(user_ids=user_idxs, item_ids=item_idxs)

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
        .map(args["item_idx_to_id_map"])
    )
    user_item_recommendations_df["user_id"] = (
        user_item_recommendations_df["user_idx"]
        .astype("str")
        .map(args["user_idx_to_id_map"])
    )

    return user_item_recommendations_df[["user_id", "item_id", "score"]]


def get_item_item_recommendations(
    model: lightfm.LightFM,
    args: dict,
    item_ids: list = None,
    item_features=None,
) -> pd.DataFrame:
    """
    gets similar items to a given item index inside user-item interaction matrix
    NB by default it won't use item features,however if item features are provided
    it will use them to get similar items

    :param args:
    :param model:
    :param item_ids:
    :param item_features:

    :return:
    """
    # todo make sure its not slow across larger data
    # todo break into smaller functions

    n_recommendations = args["n_recommendations"]

    similar_items_dfs = []

    item_idx_to_id_map = args["item_idx_to_id_map"]

    if item_ids:
        # filter out item_ids that are not in the request
        item_idx_to_id_map = {
            key: val
            for key, val in args["item_idx_to_id_map"].items()
            if val in item_ids
        }

    for item_idx, item_id in item_idx_to_id_map.items():
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

        # ensure n_recommendations is not greater than number of recommendations
        N = min(len(scores), n_recommendations)

        # get the top N items
        best = np.argpartition(scores, -N)

        # sort the scores
        rec = sorted(
            zip(best, scores[best] / item_norms[item_idx]), key=lambda x: -x[1]
        )

        intermediate_df = (
            pd.DataFrame(rec, columns=["item_idx", "score"])
            .tail(-1)  # remove the item itself
            .head(N)
        )

        intermediate_df["item_id_one"] = item_id
        similar_items_dfs.append(intermediate_df)

    similar_items_df = pd.concat(similar_items_dfs, ignore_index=True)
    similar_items_df["item_idx"] = similar_items_df["item_idx"].astype("str")

    similar_items_df["item_id_two"] = similar_items_df["item_idx"].map(
        args["item_idx_to_id_map"]
    )

    return similar_items_df[["item_id_one", "item_id_two", "score"]]
