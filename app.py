import streamlit as st
import requests
import json
import pandas as pd
import numpy as np
st.set_page_config(layout="wide")
st.title(':green[Shopify ] :blue[product scraper]')
url=st.text_input('',placeholder='Enter the URL of JSON page of shopify store ....',)
product_id=[]
handle=[]
title=[]
vendor=[]
published=[]
body_html=[]
Type=[]
tags=[]
variants_gram=[]
variants_sku=[]
img_src=[]
product_id_variants=[]
variants_inventory_qty=[]
variants_inventory_policy=[]
variants_inventory_tracker=[]
variants_inventory_service=[]
variants_price=[]
variants_compare_at_price=[]
variants_requires_shipping=[]
variants_taxable=[]
variants_weight_unit=[]
images_product_id=[]
images_position=[]
images_widhth=[]
images_height=[]
option_product_id=[]


def fetch_product(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        data_general = pd.DataFrame()
        for i in json_data['products']:
            product_id.append(i['id'])
            handle.append(i['handle'])
            try:
                title.append(i['title'])
            except:
                title.append('')
            try:
                body_html.append(i['body_html'])
            except:
                title.append('')
            vendor.append(i['vendor'])
            try:
                Type.append(i['product_type'])
            except:
                Type.append('')
            try:
                tags.append(i['tags'])
            except:
                tags.append([])
        data_general['product_id'] = product_id
        data_general['Handle'] = handle
        data_general['Title'] = title
        data_general['Body (HTML)'] = body_html
        data_general['Vendor'] = vendor
        data_general['Type'] = Type
        data_general['Tags']=tags
        data_general['Published'] = 'True'
        new_tags = []
        for i in data_general['Tags']:
            new_tags.append(','.join(i))
        data_general['Tags'] = new_tags


        data_images = pd.DataFrame()
        for i in json_data['products']:
            for j in i['images']:
                images_product_id.append(j['product_id'])
                img_src.append(j['src'])
                images_position.append(j['position'])
        data_images['product_id'] = images_product_id
        data_images['Image Src'] = img_src
        data_images['Image Position'] = images_position

        option_name_dict = {}
        option_value_dict = {}

        # Iterate over the JSON data and dynamically create lists
        for product in json_data['products']:
            for count, option in enumerate(product['options'], start=1):
                option_name_key = f'Option{count} Name'
                option_value_key = f'Option{count} Value'

                # Initialize lists if they do not exist in the dictionary
                if option_name_key not in option_name_dict:
                    option_name_dict[option_name_key] = []
                if option_value_key not in option_value_dict:
                    option_value_dict[option_value_key] = []

                # Fill the lists with the option names and values
                for value in option['values']:
                    option_product_id.append(product['id'])
                    option_name_dict[option_name_key].append(option['name'])
                    option_value_dict[option_value_key].append(value)

        # Combine the dictionaries
        combined_dict = {**option_name_dict, **option_value_dict}

        # Convert to DataFrame

        data_options = pd.DataFrame.from_dict(combined_dict, orient='index').transpose()
        data_options['product_id'] = option_product_id

        dfs = [data_general, data_options, data_images]

        # Initialize the merged DataFrame with the first DataFrame
        merged_df = dfs[0]

        # Loop through the list of DataFrames and merge them
        for df in dfs[1:]:
            merged_df = merged_df.merge(df, on='product_id')

        merged_df = merged_df.drop('product_id', axis=1)
        merged_df.drop_duplicates(inplace=True)

        merged_df.loc[
            merged_df.duplicated(subset=['Handle'], keep='first'), ['Title', 'Body (HTML)', 'Vendor', 'Type', 'Tags',
                                                                    'Published', option_name_key]] = ''

        def replace_duplicates_with_null(lst):
            unique_values = []
            new_list = []
            for item in lst:
                if item not in unique_values:
                    unique_values.append(item)
                    new_list.append(item)
            # Append NaNs only after all unique values are processed
            new_list += [np.nan] * (len(lst) - len(new_list))
            return new_list

        merged_df[option_value_key] = merged_df.groupby('Handle')[option_value_key].transform(
            replace_duplicates_with_null)
        merged_df['Image Src'] = merged_df.groupby('Handle')['Image Src'].transform(replace_duplicates_with_null)
        merged_df['Image Position'] = merged_df.groupby('Handle')['Image Position'].transform(
            replace_duplicates_with_null)
        merged_df.dropna(subset=[option_value_key, 'Image Src'], how='all', inplace=True)

        return merged_df


    except requests.exceptions.RequestException as e:
         return pd.DataFrame()


if url:
     df=fetch_product(url)
     st.download_button('Download csv file', df.to_csv(), mime='text/csv')
     st.dataframe(df,use_container_width=True,selection_mode='multi-row', height=1000)