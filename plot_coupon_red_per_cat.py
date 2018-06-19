#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 19:32:36 2018

@author: byronkim
"""
import sys
import numpy as np
import pandas as pd
from collections import Counter, defaultdict
import csv
import math
import operator
import os
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
import time

sys.path.append("/Users/byronkim/Documents/SPRING_SEMESTER/MGT6203/FInalProject/data_analytics_inbusiness/")

from feat_eng import *
from modeling import *
from create_pred_set import *

os.chdir("/Users/byronkim/Documents/SPRING_SEMESTER/MGT6203/FInalProject/data_analytics_inbusiness/")

def get_households_for_campaigns(df_campaign_table, df_campaign_desc):
    #get subset from campaign table to get the households for the campaign
    hh_start_dates = df_campaign_table.merge(df_campaign_desc, on='CAMPAIGN', how='left')
    hh_start_dates = hh_start_dates.sort_values(['household_key', 'START_DAY'])
    return hh_start_dates.drop_duplicates(['household_key'], keep="first")

def get_campaigns_for_coupon(coupon_Id, df_coupon):
    subset = df_coupon[df_coupon['COUPON_UPC'] == coupon_Id]
    return subset['CAMPAIGN'].unique()

def get_transactions_for_hh_within(df_transactions, hh_start_dates, df_coupon_red):
    trans_merge = df_transactions.merge(hh_start_dates, on='household_key', how='left')
    trans_merge['START_DAY'].fillna(10000, inplace=True)
    trans_merge['END_DAY'].fillna(0, inplace=True)
    trans_filtered = trans_merge[(trans_merge['DAY'].astype(float) >= trans_merge['START_DAY']) & (
                trans_merge['DAY'].astype(float) <= trans_merge['END_DAY'])]
    trans_filtered['label'] = 0
    print(trans_filtered.size)
    
    for coup in df_coupon_red['COUPON_UPC'].unique():
        subset = df_coupon[df_coupon['COUPON_UPC'] == coup]
        product_list = subset['PRODUCT_ID'].unique()
        print("Products associated with the coupon: "+ str(len(product_list)))
        trans_filtered['label'] = trans_filtered.apply(lambda row: 1 if row['PRODUCT_ID'] in product_list else 0,
                                                   axis=1)
        print(trans_filtered.size)
    trans_filtered = trans_filtered[trans_filtered['label'] == 1]
    return trans_filtered[['household_key', 'PRODUCT_ID', 'CAMPAIGN']], list(trans_filtered['household_key'].unique())

def get_products_for_coupon(coupon_Id, df_coupon):
    subset = df_coupon[df_coupon['COUPON_UPC'] == coupon_Id]
    return subset['PRODUCT_ID'].unique()

if __name__ == "__main__":


    print("Reading coupon data...")
    df_coupon = pd.read_csv('coupon.csv', dtype={'COUPON_UPC': str, 'CAMPAIGN': str, 'PRODUCT_ID': str})
    df_coupon_red = pd.read_csv('coupon_redempt.csv', dtype={'HOUSEHOLD_KEY': str, 'DAY': str, 'COUPON_UPC': str, 'CAMPAIGN': str})
    print(len(df_coupon_red.COUPON_UPC.unique()))

    print("Reading in campaign_table and campaign_desc...")
    df_campaign_table = pd.read_csv('campaign_table.csv', dtype={'household_key': str, 'CAMPAIGN': str})
    df_campaign_desc = pd.read_csv('campaign_desc.csv', dtype={'CAMPAIGN': str})

    hh_start_dates = get_households_for_campaigns(df_campaign_table, df_campaign_desc)
    del df_campaign_table
    hh_start_dates.drop(['DESCRIPTION_x', 'DESCRIPTION_y'], axis=1, inplace=True)


    print("Reading in transactions... it's huge")
    df_transactions = pd.read_csv('transaction_data.csv', dtype={'BASKET_ID': str, 'PRODUCT_ID': str, 'household_key': str, 'DAY': str})
    print("lenght of all transactions: "+str(len(df_transactions)))
    
    #transactions_within_campaign, households_campaign_list = get_transactions_for_hh_within(df_transactions, hh_start_dates, df_coupon_red)
    
    print("Read in product")
    df_product = pd.read_csv('product.csv', dtype={'PRODUCT_ID': str, 'DEPARTMENT': str, 'COMMODITY_DESC': str, 'SUB_COMMODITY_DESC': str, 'MANUFACTURER': str, 'BRAND': str, 'CURR_SIZE_OF_PRODUCT': str})
    print(df_product.DEPARTMENT.unique(), len(df_product.DEPARTMENT.unique()))

    
    #redeemed_coup_with_prod = transactions_within_campaign.merge(df_product, on="PRODUCT_ID", how="left")
    #redeemed_coup_with_prod_grouped = redeemed_coup_with_prod.groupby("DEPARTMENT").size().reset_index(name='counts')
    
    df_coupon_red_merged = df_coupon_red.merge(df_coupon, on=["COUPON_UPC","CAMPAIGN"], how='left')
    df_coupon_red_merged = df_coupon_red_merged.merge(df_product, on=["PRODUCT_ID"], how='left')
    df_coupon_red_merged = df_coupon_red_merged.groupby("DEPARTMENT").size().reset_index(name='counts')
    import plotly as py
    import plotly.graph_objs as go
    data = [go.Bar(
            x=df_coupon_red_merged['DEPARTMENT'],
            y=df_coupon_red_merged['counts'])]
    #51800000050
    py.offline.plot(data)
    