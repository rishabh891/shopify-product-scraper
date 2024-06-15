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
product_id_options = []
option1_name = []
option1_value = []
option2_name = []
option2_value = []
option3_name = []
option3_value = []

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
            for i in json_data['products']:
                if len(i['images']) > 0:
                    for j in i['images']:
                        images_product_id.append(j['product_id'])
                        img_src.append(j['src'])
                        images_position.append(j['position'])
                else:
                    images_product_id.append(i['id'])
                    img_src.append('')
                    images_position.append('')

        data_images['product_id'] = images_product_id
        data_images['Image Src'] = img_src
        data_images['Image Position'] = images_position


        # Extract data from JSON into lists
        for product in json_data['products']:
            product_id_options.append(product['id'])

            option1_name.append(product['options'][0]['name'])
            option1_value.append(product['options'][0]['values'])

            if len(product['options']) > 1:
                option2_name.append(product['options'][1]['name'])
                option2_value.append(product['options'][1]['values'])
            else:
                option2_name.append(None)
                option2_value.append(None)

            if len(product['options']) > 2:
                option3_name.append(product['options'][2]['name'])
                option3_value.append(product['options'][2]['values'])
            else:
                option3_name.append(None)
                option3_value.append(None)

        # Create DataFrame
        data_options = pd.DataFrame({
            'product_id': product_id_options,
            'Option1 Name': option1_name,
            'Option1 Value': option1_value,
            'Option2 Name': option2_name,
            'Option2 Value': option2_value,
            'Option3 Name': option3_name,
            'Option3 Value': option3_value,
        })

        # Explode the lists in Option Value columns into separate rows
        data_options = data_options.explode('Option1 Value').explode('Option2 Value').explode('Option3 Value')

        # Reset index
        data_options.reset_index(drop=True, inplace=True)

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
                                                                    'Published', 'Option1 Name', 'Option2 Name' ,'Option3 Name']] = ''
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

        merged_df['Option1 Value'] = merged_df.groupby('Handle')['Option1 Value'].transform(
            replace_duplicates_with_null)
        merged_df['Option2 Value'] = merged_df.groupby('Handle')['Option2 Value'].transform(
            replace_duplicates_with_null)
        merged_df['Option3 Value'] = merged_df.groupby('Handle')['Option3 Value'].transform(
            replace_duplicates_with_null)

        merged_df['Image Src'] = merged_df.groupby('Handle')['Image Src'].transform(replace_duplicates_with_null)
        merged_df['Image Position'] = merged_df.groupby('Handle')['Image Position'].transform(
            replace_duplicates_with_null)

        merged_df.dropna(subset=['Option1 Value','Option2 Value', 'Option3 Value', 'Image Src'], how='all', inplace=True)

        return merged_df


    except requests.exceptions.RequestException as e:
         return pd.DataFrame()


if url:
     df=fetch_product(url)
     st.download_button('Download csv file', df.to_csv(), mime='text/csv')
     st.dataframe(df.reset_index(drop=True),use_container_width=True,selection_mode='multi-row', height=1000)