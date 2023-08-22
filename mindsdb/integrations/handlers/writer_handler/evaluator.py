from scipy.spatial import distance


def calculate_cosine_similarity(
    context_embeddings: list, retrieved_embeddings: list
) -> float:
    cosine_sim = 1 - distance.cosine(context_embeddings, retrieved_embeddings)

    return cosine_sim


def accuracy(cosine_similarity: float, threshold: float = 0.7) -> bool:
    return cosine_similarity >= threshold
