import requests
import pandas as pd
import time

# Define the API endpoint URL
api_url = "https://data.cincinnati-oh.gov/resource/k59e-2pvf.json"  

# Set your parameters (limit and offset)
limit = 1000
offset = 0
all_data = []  # To store the combined data

# Loop until all pages are retrieved
while True:
    # Define the parameters for the current request
    params = {
        '$limit': limit,
        '$offset': offset,
    }

    # Display the current offset being processed
    print(f"Fetching data with offset: {offset}")

    try:
        # Make the API request
        response = requests.get(api_url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()

            # If no data is returned, stop the loop
            if not data:
                print("All data retrieved.")
                break

            # Append the data to the all_data list
            all_data.extend(data)

            # Increment the offset for the next page
            offset += limit

            # Sleep for a short time to avoid overloading the server
            time.sleep(2)
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        break

# Convert the combined data to a DataFrame
df = pd.DataFrame(all_data)

# Check the first few rows to ensure data retrieval was successful
print(df.head())
print(f"Retrieved {len(df)} rows in total.")
df.to_csv("cincinati_final.csv")