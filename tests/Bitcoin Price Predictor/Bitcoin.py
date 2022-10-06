import pandas as pd
from sklearn import linear_model

data=pd.read_csv("Bitcoin.csv")
# with sklearn
X = data[['Open','High','Low']] # here we have 3 variables for multiple regression.
Y = data['Close']

regr = linear_model.LinearRegression()
regr.fit(X, Y)

print('Intercept: \n', regr.intercept_)
print('Coefficients: \n', regr.coef_)


Open=int(input("Open:"))
High=int(input("High:"))
Low=int(input("Low:"))

print ('Bitcoin Price', regr.predict([[Open,High,Low]]))
