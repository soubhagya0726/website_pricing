import warnings
import pandas as pd
import numpy as np
import re
import requests
import streamlit as st
import os
import math

warnings.filterwarnings("ignore")

# Step 1: Process Data from the Web CSV Link (for price update)
def process_data_from_csv(api_csv, web_csv_url, store_name):
    st.header(f"Step 1: Process Data from {store_name}")

    st.info(f"Reading product data from {api_csv}...")

    try:
        # Read the product data (api_df) directly from the API CSV link
        api_df = pd.read_csv(api_csv)
        api_df = api_df.rename(columns={'SKU': 'Style', 'Tags': 'oldTags'})

        # Filter data for the specific store
        api_df_store = api_df[api_df['Store'] == store_name]

        # Read the price update data directly from the web CSV link
        pricing_s = pd.read_excel(web_csv_url)
        pricing_s['New SP'] = pricing_s['New SP'].astype(int)
        pricing_s['MRP'] = pricing_s['MRP'].astype(int)
        api_df_merge = api_df_store.merge(pricing_s, on='Style', how='inner')

        # Example processing logic for the data, update as necessary
        api_df_merge = api_df_merge.rename(columns={'Product ID': 'productId', 'Variant ID': 'variantId', 'New SP': 'newPrice', 'MRP': 'compareAtPrice'})

        api_df_merge1 = api_df_merge[['productId', 'variantId', 'newPrice', 'compareAtPrice', 'oldTags']]

        # Calculate discount and add new tag based on the discount value
        api_df_merge1['discount'] = (1 - (api_df_merge1['newPrice'] / api_df_merge1['compareAtPrice'])) * 100
        api_df_merge1['discount'] = api_df_merge1['discount'].fillna(0)
        api_df_merge1['discount'] = api_df_merge1['discount'].astype(int)

        # Define discount thresholds
        thresholds = [10, 20, 30, 40, 50, 60, 70, 80, 90]
        api_df_merge1['tags_test'] = api_df_merge1['discount'].apply(lambda x: [f'{threshold}% and Above' for threshold in thresholds if x >= threshold])
        api_df_merge1['tags_test'] = api_df_merge1['tags_test'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        # Clean and flatten the old tags
        def clean_and_flatten_column(column):
            def flatten(lst):
                flat_list = []
                for item in lst:
                    if isinstance(item, list):  # Recursive flattening
                        flat_list.extend(flatten(item))
                    else:
                        flat_list.append(item)
                return flat_list

            def clean_string(s):
                return re.sub(r'[\'"\[\]\\]+', '', s).strip()

            def process_row(row):
                if isinstance(row, list):
                    return [clean_string(item) for item in flatten(row)]
                elif isinstance(row, str):
                    return [clean_string(row)]
                else:
                    return row

            return column.apply(process_row)

        api_df_merge1['oldtags2'] = clean_and_flatten_column(api_df_merge1['oldTags'])

        # Remove percentage elements from the tags
        def remove_percentage_elements(tags):
            if isinstance(tags, list):
                tags = ', '.join(tags)
            elif not isinstance(tags, str):
                return tags

            elements = [e.strip() for e in tags.split(',')]
            elements = [e for e in elements if not re.match(r"\d+% and Above", e)]
            return ', '.join(elements)

        api_df_merge1['tags2'] = api_df_merge1['oldtags2'].apply(remove_percentage_elements)

        api_df_merge1['tags'] = api_df_merge1.apply(lambda row: ','.join(row['tags2'].split(',')) if row['tags_test'] == '' else row['tags_test'] if row['tags2'] == '' else ','.join(row['tags2'].split(',')) + ',' + row['tags_test'], axis=1)

        #api_df_merge1['compareAtPrice'] = api_df_merge1['compareAtPrice'].where(api_df_merge1['compareAtPrice'] != api_df_merge1['newPrice'],  np.nan)
        api_df_merge1['compareAtPrice'] = pd.to_numeric(api_df_merge1['compareAtPrice'])

        # Final dataframe with necessary columns
        api_df_merge2 = api_df_merge1[['productId', 'variantId', 'newPrice', 'compareAtPrice', 'tags']]
#         # Limit rows for shopforaurelia.com to avoid 413 error
#         if store_name == "shopforaurelia.com":
#             st.warning("Limiting data to 400 rows for shopforaurelia.com to avoid 413 Payload Too Large error.")
#             api_df_merge2 = api_df_merge2.head(400)

        # Display the final merged dataframe
        st.dataframe(api_df_merge2)

        # Save the processed data to a unique CSV file for the current store
        processed_filename = f'processed_data_{store_name}.csv'
        api_df_merge2.to_csv(processed_filename, index=False)

        return processed_filename, api_df_merge2

    except Exception as e:
        st.error(f"Failed to process data: {e}")
        return None, None


# Upload processed data
# def upload_processed_data(df, store_name, file_path):
#     st.header(f"Step 3: Upload the Data for {store_name}")
#     if df is not None:
#         st.info(f"Uploading the processed data for {store_name}...")

#         # Define the API URL for uploading
#         if store_name == "wforwoman.com":  # Example store with a specific URL format
#             upload_api_url = f"https://shopify.{store_name}/price_update/bulkedit"
#         else:
#             upload_api_url = f"https://shopify.{store_name}/priceupdate/bulkedit"

#         # Upload the file using the API
#         try:
#             with open(file_path, "rb") as file:
#                 # Bypass SSL verification by setting verify=False
#                 response = requests.post(upload_api_url, files={"file": file}, verify=False)

#             # Display the API response
#             if response.status_code == 200:
#                 st.success(f"Data uploaded successfully for {store_name}!")
#                 st.json(response.json())
#             else:
#                 st.error(f"Failed to upload data for {store_name}. Status code: {response.status_code}")
#                 st.json(response.text)
#         except requests.exceptions.RequestException as e:
#             st.error(f"Error during upload: {e}")
#     else:
#         st.error(f"No data to upload for {store_name}. Please complete Step 1.")
#########
# Step 3: Upload processed data with batching
def upload_processed_data(df, store_name, original_file_path):
    st.header(f"Step 3: Upload the Data for {store_name}")

    if df is None or df.empty:
        st.error(f"No data to upload for {store_name}. Please complete Step 1.")
        return

    st.info(f"Uploading the processed data for {store_name}...")

    # API URL
    if store_name == "wforwoman.com":
        upload_api_url = f"https://shopify.{store_name}/price_update/bulkedit"
    else:
        upload_api_url = f"https://shopify.{store_name}/priceupdate/bulkedit"

    batch_size = 1000
    total_batches = math.ceil(len(df) / batch_size)

    for i in range(total_batches):
        start = i * batch_size
        end = start + batch_size
        batch_df = df.iloc[start:end]

        # Save batch
        batch_file_path = f"{original_file_path.replace('.csv', '')}_batch_{i+1}.csv"
        batch_df.to_csv(batch_file_path, index=False)

        st.info(f"Uploading batch {i+1} of {total_batches} ({len(batch_df)} records)...")

        try:
            with open(batch_file_path, "rb") as file:
                response = requests.post(upload_api_url, files={"file": file}, verify=False)

            if response.status_code == 200:
                st.success(f"Batch {i+1} uploaded successfully!")
                try:
                    st.json(response.json())
                except ValueError:
                    st.text("Upload successful, but server did not return valid JSON.")
            else:
                st.error(f"Batch {i+1} failed. Status code: {response.status_code}")
                try:
                    st.json(response.json())
                except ValueError:
                    st.text(response.text[:500])  # partial HTML if not JSON

        except requests.exceptions.RequestException as e:
            st.error(f"Error uploading batch {i+1}: {e}")

        # Remove temp file
        os.remove(batch_file_path)

    st.success(f"All batches uploaded for {store_name}.")


#####

# Main Streamlit Application
def main():
    st.title("Dynamic Pricing Sheet Update Workflow")

    store_names = ['wforwoman.com', 'shopforaurelia.com', 'elleven.in', 'wishfulbyw.com']

    # Access tokens using secrets
    access_tokens = {
        'wforwoman.com': st.secrets["access_tokens"]["wforwomen_com"],
        'shopforaurelia.com': st.secrets["access_tokens"]["shopforaurelia_com"],
        'elleven.in': st.secrets["access_tokens"]["elleven_in"],
        'wishfulbyw.com': st.secrets["access_tokens"]["wishfulbyw_com"]
    }

    api_csv = "https://research.buywclothes.com/marketing/all_products_web.csv"  # Link to the product data CSV
    #web_csv_url = "https://research.buywclothes.com/report_data/updated_pricing.xlsx"
    web_csv_url = "https://research.buywclothes.com/marketing/test_pricing.xlsx" 

    for store_name in store_names:
        # Step 1: Process data from the CSV files for each store
        file_path, data = process_data_from_csv(api_csv, web_csv_url, store_name)

        # Step 2: Upload the processed data for the current store
        if file_path and data is not None:
            upload_processed_data(data, store_name, file_path)

if __name__ == "__main__":
    main()
