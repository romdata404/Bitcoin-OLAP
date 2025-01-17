import duckdb
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn import preprocessing
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import r2_score, mean_squared_error
import statsmodels.api as sm


# Connect to an in-memory DuckDB instance
con = duckdb.connect()

# Aggregate daily inputs
daily_inputs = con.execute("""
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,
        SUM(-val) AS txid_input
    FROM 'C:/Users/rikuo/Documents/Tx_Data/postSAFE_location/post_location-*.parquet'
    WHERE type = 'input' AND location = 'China'
    GROUP BY DATE_TRUNC('day', block_timestamp)
    ORDER BY date;
""").df()



daily_inputs = daily_inputs[daily_inputs['txid_input'] > 1]


# Aggregate daily outputs
daily_outputs = con.execute("""
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,        
        SUM(val) AS txid_output
    FROM 'C:/Users/rikuo/Documents/Tx_Data/postSAFE_location/post_location-*.parquet'
    WHERE type = 'output' AND location != 'China'
    GROUP BY DATE_TRUNC('day', block_timestamp)
    ORDER BY date;
""").df()

daily_outputs = daily_outputs[daily_outputs['txid_output'] > 1]
# Merge on date
merged_data = pd.merge(daily_inputs, daily_outputs, on='date', how='inner')

#linear regression
# 1. Sample the data for visualization while keeping full dataset for model
def smart_sampling(X, y, max_points=1000000000):
    total_points = len(X)
    
    if total_points > max_points:
        # Stratified sampling for better representation
        percentiles = np.linspace(0, 100, 20)  # Create 20 bins
        bins = np.percentile(X, percentiles)
        indices = []
        
        # Sample from each bin
        points_per_bin = max_points // 20
        for i in range(len(bins)-1):
            mask = (X >= bins[i]) & (X < bins[i+1])
            bin_indices = np.where(mask)[0]
            if len(bin_indices) > points_per_bin:
                bin_indices = np.random.choice(bin_indices, points_per_bin, replace=False)
            indices.extend(bin_indices)
        
        return X[indices], y[indices]
    return X, y

# 2. Optimized plotting function
class plot_regression:
    def logistic(X, y, sample_size=100000000):

        log_x = np.log(X)
        log_y = np.log(y)

        log_x_with_const = sm.add_constant(log_x)
        
        model = sm.OLS(log_y, log_x_with_const)
        results = model.fit()

        print(results.summary())
        # Predict log_y values
        log_y_pred = results.predict(log_x_with_const)

        plt.figure(figsize=(12, 8)) 
        # Plot regression line 
        plt.scatter(log_x, log_y, alpha=0.8, color='blue', s=1) #label='Actual Data (Sampled)'
        plt.plot(log_x, log_y_pred, color='red') #, label='Fitted Line'
        plt.xlim([5, 12])
        plt.ylim([5, 12])
        
        # Add labels and title
        '''plt.title('Outflow from China vs Inflow to Abroad Log Regression Post-SAFE Analysis', fontsize=14)
        plt.xlabel('Outflow from China (Log(BTC/Day))', fontsize=12)
        plt.ylabel('Inflow to Abroad (Log(BTC/Day))', fontsize=12)'''
        plt.legend(fontsize=10)

        equation = f'log(y) = {results.params[1]:.2f} * log(X) + {results.params[0]:.2f}'
        plt.text(0.05, 0.85, f'Equation: {equation}\nR² = {results.rsquared:.3f}\nRMSE = {np.sqrt(mean_squared_error(log_y, log_y_pred)):.3f}',
            transform=plt.gca().transAxes,
            bbox=dict(facecolor='white', alpha=0.8))
        plt.show()
#\nT Statistic (Intercept, Slope) = {results.tvalues[0]:.3f}, {results.tvalues[1]:.3f}\nP Value (Intercept, Slope) = {results.pvalues[0]:.3f}, {results.pvalues[1]:.3f}',

# 3. Main analysis pipeline
def analyze_large_dataset(merged_data, sample_size=10000000000):
    # Convert to numpy arrays for better performance
    X = merged_data['txid_input'].values.reshape(-1, 1)
    y = merged_data['txid_output'].values
    
    # Create visualization with sampled data
    plot = plot_regression.logistic(X, y, sample_size)
    
    return plot, model

# 4. Execute the analysis
# Optional: Use subset of columns to reduce memory usage
merged_data_subset = merged_data[['txid_input', 'txid_output']].copy()

# Remove any invalid values
merged_data_subset = merged_data_subset.dropna()

print(f"Total data points: {len(merged_data_subset)}")
plot, model = analyze_large_dataset(merged_data_subset)
plot.show()
    


con.close()