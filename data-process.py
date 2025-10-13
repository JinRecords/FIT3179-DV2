import pandas as pd

# Make sure the CSV file is in the same directory as this script
file_path = '_202302280412050_2000-2021-mean-temperature-rainfall-volume-and-mean-relative-humidity-malaysia-(1).csv'

try:
    df = pd.read_csv(file_path)

    # Clean column names by stripping leading/trailing whitespace
    df.columns = df.columns.str.strip()

    # Rename columns for easier access
    df.rename(columns={
        'State': 'STATE',
        'Year': 'YEAR',
        'Number of Days of Rainfall': 'NUMBER OF DAYS OF RAINFALL',
        'Total Rainfall in millimetres': 'ANNUAL RAINFALL',
        'Selected meteorological station': 'Selected meteorological station'
    }, inplace=True)

    # Create a copy for processing
    processed_df = df.copy()

    # Handle Kuala Lumpur data
    kl_df = processed_df[processed_df['Selected meteorological station'].str.contains('Subang', case=False, na=False)].copy()
    kl_df['STATE'] = 'Kuala Lumpur'

    # Remove Subang data from the original dataframe to avoid duplication
    processed_df = processed_df[~processed_df['Selected meteorological station'].str.contains('Subang', case=False, na=False)]

    # Concatenate the processed data with the new Kuala Lumpur data
    final_df = pd.concat([processed_df, kl_df])

    # --- FIX STARTS HERE ---
    # Convert rainfall columns to numeric, forcing errors into 'NaN' (Not a Number)
    # This will clean up problematic text like '205.0200.0'
    final_df['NUMBER OF DAYS OF RAINFALL'] = pd.to_numeric(final_df['NUMBER OF DAYS OF RAINFALL'], errors='coerce')
    final_df['ANNUAL RAINFALL'] = pd.to_numeric(final_df['ANNUAL RAINFALL'], errors='coerce')
    # --- FIX ENDS HERE ---

    # Group by State and Year and calculate the mean for rainfall data
    # The .mean() function will automatically ignore the NaN values
    aggregated_df = final_df.groupby(['STATE', 'YEAR']).agg({
        'NUMBER OF DAYS OF RAINFALL': 'mean',
        'ANNUAL RAINFALL': 'mean'
    }).reset_index()

    # Select and reorder the final columns
    output_df = aggregated_df[['STATE', 'YEAR', 'NUMBER OF DAYS OF RAINFALL', 'ANNUAL RAINFALL']]

    # Save the processed data to a new CSV file
    output_file_path = 'processed_rainfall_data.csv'
    output_df.to_csv(output_file_path, index=False)

    print(f"✅ Success! Processed data saved to {output_file_path}")
    print("\nHere's a preview of the processed data:")
    print(output_df.head())

except FileNotFoundError:
    print(f"❌ Error: The file '{file_path}' was not found.")
    print("Please make sure the CSV file is in the same directory as the script and that the filename matches.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
