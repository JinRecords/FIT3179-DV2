
import pandas as pd
import numpy as np

# Load the dataset
df = pd.read_csv('_202302280412050_2000-2021-mean-temperature-rainfall-volume-and-mean-relative-humidity-malaysia-(1).csv')

# Clean column names
df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('-', '_').str.lower()

# Define West and East Malaysia states
west_malaysia_states = [
    'Johor', 'Kedah', 'Kelantan', 'Melaka', 'Negeri Sembilan',
    'Pahang', 'Perak', 'Perlis', 'Pulau Pinang', 'Selangor', 'Terengganu'
]
east_malaysia_states = ['Sabah', 'Sarawak', 'Wilayah Persekutuan Labuan']

# Function to classify states
def get_region(state):
    if state in west_malaysia_states:
        return 'West'
    elif state in east_malaysia_states:
        return 'East'
    else:
        return 'Unknown'

# Apply the function to create a 'region' column
df['region'] = df['state'].apply(get_region)

# Column to process
rainfall_col = 'total_rainfall_in_millimetres'

# Replace non-numeric placeholders with NaN
df[rainfall_col] = pd.to_numeric(df[rainfall_col], errors='coerce')

# Drop rows with missing values in the rainfall column
df.dropna(subset=[rainfall_col], inplace=True)

# Group by year and region, then calculate the mean of the total_rainfall_in_millimetres
yearly_regional_rainfall = df.groupby(['year', 'region'])[rainfall_col].mean().reset_index()

# Pivot the table to have regions as columns
pivot_df = yearly_regional_rainfall.pivot(index='year', columns='region', values=rainfall_col).reset_index()
pivot_df.columns.name = None
pivot_df = pivot_df.rename(columns={'West': 'West_Avg_Rainfall', 'East': 'East_Avg_Rainfall'})


# Save the cleaned data
pivot_df.to_csv('cleaned_east_west.csv', index=False)

print("Data cleaning and processing for rainfall complete. Output saved to cleaned_east_west.csv")
print(pivot_df.head())
