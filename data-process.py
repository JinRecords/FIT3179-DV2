import pandas as pd

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
    # Isolate the source data from KLIA and Melaka stations
    ns_source_stations = ['Kuala Lumpur International Airport (KLIA), Sepang', 'Melaka']
    ns_source_df = df[df['Selected meteorological station'].isin(ns_source_stations)].copy()

    # Clean the numeric columns for the source data
    ns_source_df['NUMBER OF DAYS OF RAINFALL'] = pd.to_numeric(ns_source_df['NUMBER OF DAYS OF RAINFALL'], errors='coerce')
    ns_source_df['ANNUAL RAINFALL'] = pd.to_numeric(ns_source_df['ANNUAL RAINFALL'], errors='coerce')

    # Calculate the yearly average for Negeri Sembilan
    negeri_sembilan_df = ns_source_df.groupby('YEAR').agg({
        'NUMBER OF DAYS OF RAINFALL': 'mean',
        'ANNUAL RAINFALL': 'mean'
    }).reset_index()
    negeri_sembilan_df['STATE'] = 'Negeri Sembilan' # Assign the new state name

    # --- 2. Prepare the Rest of the Data ---
    # Start with a fresh copy of the main dataframe
    processed_df = df.copy()

    # Exclude the data that was just used for Negeri Sembilan
    processed_df = processed_df[~processed_df['Selected meteorological station'].isin(ns_source_stations)]

    # --- 3. Special Handling for Kuala Lumpur (as before) ---
    kl_df = processed_df[processed_df['Selected meteorological station'].str.contains('Subang', na=False)].copy()
    kl_df['STATE'] = 'Kuala Lumpur'
    
    # Remove Subang from the main processed dataframe
    processed_df = processed_df[~processed_df['Selected meteorological station'].str.contains('Subang', na=False)]

    # --- 4. Combine and Aggregate All Other States ---
    # Combine the main data with the specially handled Kuala Lumpur data
    other_states_df = pd.concat([processed_df, kl_df])

    # Clean the numeric columns for all remaining data
    other_states_df['NUMBER OF DAYS OF RAINFALL'] = pd.to_numeric(other_states_df['NUMBER OF DAYS OF RAINFALL'], errors='coerce')
    other_states_df['ANNUAL RAINFALL'] = pd.to_numeric(other_states_df['ANNUAL RAINFALL'], errors='coerce')

    # Group the rest of the states and average their data
    aggregated_others_df = other_states_df.groupby(['STATE', 'YEAR']).agg({
        'NUMBER OF DAYS OF RAINFALL': 'mean',
        'ANNUAL RAINFALL': 'mean'
    }).reset_index()

    # --- 5. Final Combination ---
    # Combine the aggregated other states with the new Negeri Sembilan data
    final_df = pd.concat([aggregated_others_df, negeri_sembilan_df], ignore_index=True)

    # Select and reorder the final columns and sort the data
    output_df = final_df[['STATE', 'YEAR', 'NUMBER OF DAYS OF RAINFALL', 'ANNUAL RAINFALL']]
    output_df = output_df.sort_values(by=['STATE', 'YEAR']).reset_index(drop=True)

    # Save the processed data to a new CSV file
    output_file_path = 'processed_rainfall_data.csv'
    output_df.to_csv(output_file_path, index=False)

    print(f"✅ Success! Processed data saved to {output_file_path}")
    print("\nHere's a preview of the processed data, including the new Negeri Sembilan entry:")
    # Show a snippet that includes Negeri Sembilan to confirm the logic worked
    print(output_df[output_df['STATE'].isin(['Melaka', 'Negeri Sembilan', 'Selangor'])].head(10))

except FileNotFoundError:
    print(f"❌ Error: The file '{file_path}' was not found.")
    print("Please make sure the CSV file is in the same directory as the script and that the filename matches.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
