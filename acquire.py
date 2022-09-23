import requests
import pandas as pd
import numpy as np
import os

def get_items():

    if os.path.isfile('items.csv'):
        return pd.read_csv('items.csv', index_col=0)
        
    else:
        
        items_response = requests.get('https://python.zgulde.net/api/v1/items')
        items_data = items_response.json()
        items = pd.DataFrame(items_data['payload']['items'])
        base_url = 'https://python.zgulde.net'

        items_response2 = requests.get(base_url + items_data['payload']['next_page'])
        items_data2 = items_response2.json()

        items2 = pd.DataFrame(items_data2['payload']['items'])

        items_repsonse3 = requests.get(base_url + items_data2['payload']['next_page'])

        items_data3 = items_repsonse3.json()

        items3 = pd.DataFrame(items_data3['payload']['items'])

        #Add the results to my items dataframe
        items = pd.concat([items,items2, items3]).reset_index()
        items = items.drop(columns='index')
    return items

def get_stores():

    if os.path.isfile('stores.csv'):
        return pd.read_csv('stores.csv', index_col=0)
        
    else:
        stores_response = requests.get('https://python.zgulde.net/api/v1/stores')
        stores_data = stores_response.json()
        stores = pd.DataFrame(stores_data['payload']['stores'])
    return stores


def get_sales():
    if os.path.isfile('sales.csv'):
        return pd.read_csv('sales.csv', index_col=0)
        
    else:
        sales_response = requests.get('https://python.zgulde.net/api/v1/sales')
        sales_data = sales_response.json()
        max_page_sales = sales_data['payload']['max_page']

        sales_url = 'https://python.zgulde.net/api/v1/sales?page='

        sales = pd.DataFrame(sales_data['payload']['sales'])

        for i in range(2, max_page_sales + 1):
            response = requests.get(sales_url + str(i))
            json = response.json()
            df = pd.DataFrame(json['payload']['sales'])
            sales = pd.concat([sales, df])
        sales.reset_index(inplace=True)
    return sales

def super_df():
    sales = get_sales()
    items = get_items()
    stores = get_stores()

    super_df = sales.merge(items, how='left', left_on='item', right_on='item_id')

    super_df = super_df.merge(stores, how='left', left_on='store', right_on='store_id')
    
    super_df.drop(columns=['index','item', 'sale_id', 'store', 'item_id', 'item_upc12', 'item_upc14',
                       'store_id'], inplace=True)
    return super_df

def get_opsd():
    opsd = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
    return opsd

