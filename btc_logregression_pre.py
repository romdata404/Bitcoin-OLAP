import duckdb
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import r2_score, mean_squared_error, roc_curve, auc
import statsmodels.api as sm

con = duckdb.connect()

# Aggregate daily inputs
daily_inputs = con.execute("""
    WITH infilter AS (
        SELECT *
        FROM 'C:/Users/rikuo/Documents/Tx_Data/preSAFE_location/pre_location-*.parquet'
        WHERE block_timestamp > '2018-09-24')
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,
        SUM(-val) AS txid_input
    FROM infilter
    WHERE type = 'input' AND location = 'China'
    GROUP BY DATE_TRUNC('day', block_timestamp)
    ORDER BY txid_input;
""").df()

daily_inputs = daily_inputs[daily_inputs['txid_input'] > 1]


# Aggregate daily outputs
daily_outputs = con.execute("""
    WITH outfilter AS (
        SELECT *
        FROM 'C:/Users/rikuo/Documents/Tx_Data/preSAFE_location/pre_location-*.parquet'
        WHERE block_timestamp > '2018-09-24')
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,        
        SUM(val) AS txid_output
    FROM outfilter
    WHERE type = 'output' AND location = 'Russia'
    GROUP BY DATE_TRUNC('day', block_timestamp)
    ORDER BY txid_output;
""").df()

daily_outputs = daily_outputs[daily_outputs['txid_output'] > 1]

# Merge
merged_data = pd.merge(daily_inputs, daily_outputs, on='date', how='inner')

#linear regression
#Sample the data for visualization while keeping full dataset for model
def smart_sampling(X, y, max_points=1000000000):
    total_points = len(X)
    
    if total_points > max_points:
        percentiles = np.linspace(0, 100, 20)  # Create 20 bins
        bins = np.percentile(X, percentiles)
        indices = []
        
        points_per_bin = max_points // 20
        for i in range(len(bins)-1):
            mask = (X >= bins[i]) & (X < bins[i+1])
            bin_indices = np.where(mask)[0]
            if len(bin_indices) > points_per_bin:
                bin_indices = np.random.choice(bin_indices, points_per_bin, replace=False)
            indices.extend(bin_indices)
        
        return X[indices], y[indices]
    return X, y

#plotting function
class plot_regression:

    def logistic(X, y, model, sample_size=100000000):

        log_x = np.log(X)
        log_y = np.log(y)

        log_x_with_const = sm.add_constant(log_x)
        
        model = sm.OLS(log_y, log_x_with_const)
        results = model.fit()

        print(results.summary())
        log_y_pred = results.predict(log_x_with_const)

        plt.figure(figsize=(12, 8))
          
        x_min, x_max = plt.xlim([5,12])
        plt.scatter(log_x, log_y, alpha=0.8, color='blue', s=1)

        # Define the range for the regression line
        
        x_range = np.linspace(x_min, x_max, 100)
        x_range_with_const = sm.add_constant(x_range)
        y_range_pred = results.predict(x_range_with_const)

        # Plot the extended regression line
        plt.plot(x_range, y_range_pred, color='red')

           
        
        '''plt.title('Outflow from China vs Inflow to Russia Log Regression Pre-SAFE Analysis', fontsize=14)
        plt.xlabel('Outflow from China (Log(BTC/Day))', fontsize=12)
        plt.ylabel('Inflow to Russia (Log(BTC/Day))', fontsize=12)'''
        plt.legend(fontsize=10)

        equation = f'log(y) = {results.params[1]:.2f} * log(X) + {results.params[0]:.2f}'
        plt.text(0.05, 0.85, f'Equation: {equation}\nR² = {results.rsquared:.3f}\nRMSE = {np.sqrt(mean_squared_error(log_y, log_y_pred)):.3f}',
            transform=plt.gca().transAxes,
            bbox=dict(facecolor='white', alpha=0.8))

        '''equation = f'log(y) = {results.params[1]:.2f} * log(X) + {results.params[0]:.2f}'
        plt.text(0.05, 0.85, f'Equation: {equation}\nR² = {results.rsquared:.3f}\nRMSE = {np.sqrt(mean_squared_error(log_y, log_y_pred)):.3f}\nT Statistic (Intercept, Slope) = {results.tvalues[0]:.3f}, {results.tvalues[1]:.3f}\nP Value (Intercept, Slope) = {results.pvalues[0]:.3f}, {results.pvalues[1]:.3f}',
            transform=plt.gca().transAxes,
            bbox=dict(facecolor='white', alpha=0.8))'''
        plt.show()
# main analysis
def analyze_large_dataset(merged_data, sample_size=10000000000):

    X = merged_data['txid_input'].values.reshape(-1, 1)
    y = merged_data['txid_output'].values
    
    model = LogisticRegression()
    #model.fit(X, y)
    
    plot = plot_regression.logistic(X, y, model, sample_size)
    
    print("\nRegression Statistics:")
    print(f"Coefficient (slope): {model.coef_[0]:.4f}")
    print(f"Intercept: {model.intercept_:.4f}")
    print(f"R-squared: {r2_score(y, model.predict(X)):.4f}")
    print(f"Root Mean Square Error: {np.sqrt(mean_squared_error(y, model.predict(X))):.4f}")
    
    return plot, model

try:

    merged_data_subset = merged_data[['txid_input', 'txid_output']].copy()
    
    merged_data_subset = merged_data_subset.dropna()
    
    print(f"Total data points: {len(merged_data_subset)}")
    plot, model = analyze_large_dataset(merged_data_subset)
    plot.show()
    
except Exception as e:
    print(f"An error occurred: {str(e)}")

con.close()