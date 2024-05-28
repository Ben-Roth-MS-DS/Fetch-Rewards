#load in packages
import pandas as pd
import numpy as np
import os
import datetime

#define path
path = os.getcwd()

#read in files
brands = pd.read_json(os.path.join(path, 'brands.json'), lines = True)
users = pd.read_json(os.path.join(path, 'users.json'), lines = True)
receipts = pd.read_json(os.path.join(path, 'receipts.json'), lines = True)

#clean up brands
brands['id'] = brands['_id'].apply(pd.Series)
brands.drop(columns = ['_id'], inplace = True)

brands[['id', 'ref']] = brands.cpg.apply(pd.Series)
brands['id'] = brands.id.apply(pd.Series)
brands.drop(columns = ['cpg'], inplace = True)

#clean up users
users['id'] = users['_id'].apply(pd.Series)
users.drop(columns = ['_id'], inplace = True)

users['createdDate'] = users.createdDate.apply(pd.Series)
users['createdDate'] = users.createdDate.apply(lambda x: datetime.datetime.fromtimestamp(int(x)/1000))

users['lastLogin'] = users.lastLogin.apply(pd.Series)['$date']
users['lastLogin'] = users.lastLogin.apply(lambda x: x if pd.isnull(x) else datetime.datetime.fromtimestamp(int(x)/1000))

#clean up receipts
receipts['id'] = receipts['_id'].apply(pd.Series)
receipts.drop(columns = ['_id'], inplace = True)

dateColumns = [col for col in receipts.columns if any(substring in col for substring in ['Date', 'date'])]

for column in dateColumns:
    try:
        receipts[column] = receipts[column].apply(pd.Series)
        receipts[column] = receipts[column].apply(lambda x: x if pd.isnull(x) else datetime.datetime.fromtimestamp(int(x)/1000))
    except:
        receipts[column] = receipts[column].apply(pd.Series)['$date']
        receipts[column] = receipts[column].apply(lambda x: x if pd.isnull(x) else datetime.datetime.fromtimestamp(int(x)/1000))


### convert receipts from wide to long, with each row being a receipt item ###
        
#dataframe where each column is a receipt item
receiptItems = receipts['rewardsReceiptItemList'].apply(pd.Series)

#join receipt info with receipt items
receipts_joined = pd.concat([receipts, receiptItems], axis = 1)

#convert from wide to long, where each receipt item has its own row
receipts_long = pd.melt(receipts_joined
                        , id_vars = [column for column in receipts.columns if column != 'rewardsReceiptItemList']
                        , value_vars = [column for column in receiptItems.columns])

#drop rows that don't have a receipt value (i.e. most of the receipts don't have 400 items, 
#so receipt item 400 and it's associated value will be dropped)
receipts_long = receipts_long.dropna(subset = 'value')

#convert value column of dicts to new columns with item information
itemInfo = receipts_long.value.apply(pd.Series)

#convert partnerItemId to int column
itemInfo.partnerItemId = itemInfo.partnerItemId.astype(int)

#add suffix to column name if it exists in receipts_long
itemInfo.columns = [col + 'ItemInfo' if col in receipts_long.columns else col for col in itemInfo.columns]

#join item info back into long dataframe, sort by unique identifiers, add suffix to iteminfo to remove duplicate column names
receipts_fin = pd.concat([receipts_long.drop(columns = ['value', 'variable']), itemInfo], axis = 1).\
    sort_values(['id', 'partnerItemId']).\
    reset_index(drop = True)

#create primary key
receipts_fin['primaryKey'] = receipts_fin.id + receipts_fin.partnerItemId.astype(str)

#reorder columns so receipt id and userid are first
first_columns = ['primaryKey', 'id', 'userId']
next_columns = [col for col in receipts_fin.columns if col not in first_columns]

receipts_fin = receipts_fin[first_columns + next_columns]

#save dataframes
brands.to_csv(os.path.join(path, 'brands.csv'), index=False)
users.to_csv(os.path.join(path, 'users.csv'), index=False)
receipts_fin.to_csv(os.path.join(path, 'receipts.csv'), index=False)
