import pandas as pd
import numpy as np
from datetime import datetime

# Load the CSV file
df = pd.read_csv('stock_movement.csv')

print("Original Data:")
print(df.head())

# Step 2: Clean the data

# Convert 'movement_date' to datetime format
df['movement_date'] = pd.to_datetime(df['movement_date'], errors='coerce')

# Drop rows with invalid dates
invalid_dates = df['movement_date'].isna()
if invalid_dates.any():
    print(f"Dropping {invalid_dates.sum()} rows with invalid dates")
    df = df[~invalid_dates]

# Validate quantity > 0
valid_quantity = df['quantity'] > 0
if not valid_quantity.all():
    print(f"Dropping {len(df) - valid_quantity.sum()} rows with invalid quantity")
    df = df[valid_quantity]

# Normalize movement type
df['movement_type'] = df['movement_type'].str.upper()

# Trim whitespace in 'reason'
df['reason'] = df['reason'].str.strip()

print("\nCleaned Data:")
print(df.head())

df.to_csv('cleaned_stock_movements.csv', index=False)
print("\n Cleaned data saved to 'cleaned_stock_movements.csv'")

# Current stock levels
df['net_quantity'] = np.where(df['movement_type'] == 'IN', df['quantity'], -df['quantity'])

# Group by product_id to get current stock
stock_summary = df.groupby('product_id')['net_quantity'].sum()

print("\nCurrent stock levels per product:")
print(stock_summary)

# Define sample reorder levels
reorder_levels = {
    1: 20,
    2: 15,
    3: 10,
    4: 5,
    5: 10,
    6: 12
}

stock_with_reorder = pd.DataFrame({
    'current_stock': stock_summary,
    'reorder_level': pd.Series(reorder_levels)
}).fillna(0)

stock_with_reorder['below_reorder'] = stock_with_reorder['current_stock'] < stock_with_reorder['reorder_level']

print("\n Products below reorder level:")
print(stock_with_reorder[stock_with_reorder['below_reorder']])