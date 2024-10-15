# Import necessary libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import yfinance as yf

# Step 1: Fetch Stock Data
def get_stock_data(stock_symbol, start_date, end_date):
    stock_data = yf.download(stock_symbol, start=start_date, end=end_date)
    return stock_data

# Step 2: Preprocessing the Data
def preprocess_data(stock_data):
    # We'll use the 'Close' price for prediction
    stock_data['Date'] = stock_data.index
    stock_data = stock_data[['Date', 'Close']]
    stock_data['Prediction'] = stock_data['Close'].shift(-1)  # Predict next day's Close price
    
    # Drop last row (since it will have NaN in Prediction)
    stock_data = stock_data.dropna()

    return stock_data

# Step 3: Split the data into training and testing
def prepare_train_test_data(stock_data):
    X = np.array(stock_data['Close']).reshape(-1, 1)
    y = np.array(stock_data['Prediction'])

    # Split data into 80% training and 20% testing
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    return X_train, X_test, y_train, y_test

# Step 4: Build and train the model
def train_linear_regression(X_train, y_train):
    model = LinearRegression()
    model.fit(X_train, y_train)
    return model

# Step 5: Evaluate the model
def evaluate_model(model, X_test, y_test):
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    return mse, predictions

# Step 6: Plot the results
def plot_results(stock_data, X_test, predictions):
    valid = stock_data[-len(X_test):]  # Select the same size as test data
    valid['Predictions'] = predictions

    # Plotting actual vs predicted prices
    plt.figure(figsize=(16,8))
    plt.title('Stock Price Prediction')
    plt.xlabel('Days')
    plt.ylabel('Close Price USD ($)')
    plt.plot(stock_data['Close'], label='Actual Close Price')
    plt.plot(valid['Predictions'], label='Predicted Close Price', linestyle='--')
    plt.legend()
    plt.show()

# Main function
if __name__ == "__main__":
    stock_symbol = 'AAPL'  # You can change this to any stock
    start_date = '2020-01-01'
    end_date = '2023-01-01'

    # Get stock data
    stock_data = get_stock_data(stock_symbol, start_date, end_date)

    # Preprocess data
    stock_data = preprocess_data(stock_data)

    # Prepare training and testing data
    X_train, X_test, y_train, y_test = prepare_train_test_data(stock_data)

    # Train the model
    model = train_linear_regression(X_train, y_train)

    # Evaluate the model
    mse, predictions = evaluate_model(model, X_test, y_test)
    print(f"Mean Squared Error: {mse}")

    # Plot the results
    plot_results(stock_data, X_test, predictions)
