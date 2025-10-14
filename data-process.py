import pandas as pd
import numpy as np

# Make sure the CSV file is in the same directory as this script
file_path = '_202302280412050_2000-2021-mean-temperature-rainfall-volume-and-mean-relative-humidity-malaysia-(1).csv'

try:
    df = pd.read_csv(file_path)

    # --- Initial Cleaning ---
    df.columns = df.columns.str.strip()
    df.rename(columns={
        'State': 'STATE',
        'Year': 'YEAR',
        'Number of Days of Rainfall': 'NUMBER OF DAYS OF RAINFALL',
        'Total Rainfall in millimetres': 'ANNUAL RAINFALL',
        'Selected meteorological station': 'Selected meteorological station'
    }, inplace=True)

    # --- 1. Special Handling for Negeri Sembilan ---
    # This part calculates the Negeri Sembilan average but does NOT remove the source data.
    ns_source_stations = ['Kuala Lumpur International Airport (KLIA), Sepang', 'Melaka']
    ns_source_df = df[df['Selected meteorological station'].isin(ns_source_stations)].copy()
    ns_source_df['NUMBER OF DAYS OF RAINFALL'] = pd.to_numeric(ns_source_df['NUMBER OF DAYS OF RAINFALL'], errors='coerce')
    ns_source_df['ANNUAL RAINFALL'] = pd.to_numeric(ns_source_df['ANNUAL RAINFALL'], errors='coerce')

    negeri_sembilan_df = ns_source_df.groupby('YEAR').agg({
        'NUMBER OF DAYS OF RAINFALL': 'mean',
        'ANNUAL RAINFALL': 'mean'
    }).reset_index()
    negeri_sembilan_df['STATE'] = 'Negeri Sembilan'

    # --- 2. Prepare the Rest of the Data ---
    processed_df = df.copy()
    
    # --- 3. Special Handling for Kuala Lumpur ---
    # Isolate the data for the 'Subang' station for Kuala Lumpur
    kl_df = processed_df[processed_df['Selected meteorological station'].str.contains('Subang', na=False)].copy()
    kl_df['STATE'] = 'Kuala Lumpur'
    
    # Remove 'Subang' from the main dataframe to prevent it from being processed again under 'Selangor'
    processed_df = processed_df[~processed_df['Selected meteorological station'].str.contains('Subang', na=False)]

    # --- 4. Combine and Aggregate All Other States ---
    # Note: 'processed_df' now correctly contains both the KLIA station and the Melaka station.
    # The 'kl_df' contains the re-assigned Subang data.
    other_states_df = pd.concat([processed_df, kl_df])
    
    other_states_df['NUMBER OF DAYS OF RAINFALL'] = pd.to_numeric(other_states_df['NUMBER OF DAYS OF RAINFALL'], errors='coerce')
    other_states_df['ANNUAL RAINFALL'] = pd.to_numeric(other_states_df['ANNUAL RAINFALL'], errors='coerce')

    # This will now correctly group and average all states, including Selangor and Melaka
    aggregated_others_df = other_states_df.groupby(['STATE', 'YEAR']).agg({
        'NUMBER OF DAYS OF RAINFALL': 'mean',
        'ANNUAL RAINFALL': 'mean'
    }).reset_index()

    # --- 5. Combine All Processed Data ---
    combined_df = pd.concat([aggregated_others_df, negeri_sembilan_df], ignore_index=True)

    # --- 6. Ensure All States Have All Years (2000-2021) ---
    all_states = combined_df['STATE'].unique()
    all_years = range(2000, 2022)

    master_grid = pd.MultiIndex.from_product([all_states, all_years], names=['STATE', 'YEAR'])
    master_df = pd.DataFrame(index=master_grid).reset_index()
    
    final_df = pd.merge(master_df, combined_df, on=['STATE', 'YEAR'], how='left')
    
    # --- 7. Final Formatting ---
    output_df = final_df[['STATE', 'YEAR', 'NUMBER OF DAYS OF RAINFALL', 'ANNUAL RAINFALL']]
    output_df = output_df.sort_values(by=['STATE', 'YEAR']).reset_index(drop=True)

    # Save to a new file to avoid confusion
    output_file_path = 'processed_rainfall_data_final_v2.csv'
    output_df.to_csv(output_file_path, index=False)

    print(f"✅ Success! Final corrected data saved to {output_file_path}")
    print("\nHere's a preview for Melaka, which is now correctly included:")
    print(output_df[output_df['STATE'] == 'Melaka'].head(5))

except FileNotFoundError:
    print(f"❌ Error: The file '{file_path}' was not found.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
