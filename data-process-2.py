import pandas as pd

def clean_disaster_data(input_filename, output_filename):
    """
    Cleans the disaster data from the input CSV file and saves it to a new CSV file.

    Args:
        input_filename (str): The name of the source CSV file.
        output_filename (str): The name of the file to save the cleaned data to.
    """
    try:
        # Read the source CSV file into a pandas DataFrame
        df = pd.read_csv(input_filename)

        # Select and rename the desired columns
        # Create a copy to avoid SettingWithCopyWarning
        df_cleaned = df[['Start Year', 'Disaster Subtype', 'Origin', 'Total Deaths', 'Total Affected', "Total Damage ('000 US$)"]].copy()

        # Rename the columns to the desired format
        df_cleaned.rename(columns={
            'Start Year': 'YEAR',
            "Total Damage ('000 US$)": 'Total Damage (1K USD)'
        }, inplace=True)

        # Save the cleaned DataFrame to a new CSV file
        df_cleaned.to_csv(output_filename, index=False)

        print(f"Data cleaning complete. Cleaned data saved to '{output_filename}'")
        print("\nHere is a preview of the cleaned data:")
        print(df_cleaned.head())

    except FileNotFoundError:
        print(f"Error: The file '{input_filename}' was not found.")
        print("Please make sure the file is in the same directory as the script.")
    except KeyError as e:
        print(f"Error: A required column was not found in the CSV file: {e}")
        print("Please check the column names in your source file.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Define the input and output filenames
    source_file = 'public_emdat_custom_request_2025-09-25_30f04d7a-5e31-4ed8-ad6d-30fab9e03773.csv'
    cleaned_file = 'cleaned_disaster_data.csv'

    # Run the cleaning function
    clean_disaster_data(source_file, cleaned_file)
