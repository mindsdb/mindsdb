import numpy as np

np.random.seed(0)

N_CUSTOMERS = 100
N_PRODUCTS = 5000
N_PURCHASES_MEAN = 10  # customers buy 100 articles on average

with open("transactions.csv", "w") as file:
	file.write(f"customer_id,article_id\n")  # header

	for customer_id in range(N_CUSTOMERS):
		n_purchases = np.random.poisson(lam=N_PURCHASES_MEAN)
		articles = np.random.randint(low=0, high=N_PRODUCTS, size=n_purchases)
		for article_id in articles:
			file.write(f"{customer_id},{article_id}\n")  # transaction as a row