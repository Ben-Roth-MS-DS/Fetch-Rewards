import sqlite3
import os
import pandas as pd

#get path
path = os.getcwd()

#connect to database
conn = sqlite3.connect(os.path.join(path, 'fetchDB.db'))

#add csvs to db
pd.read_csv(os.path.join(path, 'receipts.csv')).to_sql('receipts', conn)
pd.read_csv(os.path.join(path, 'users.csv')).to_sql('users', conn)
pd.read_csv(os.path.join(path, 'brands.csv')).to_sql('brands', conn)

### answer questions ###

### 1. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?

conn.execute('select avg(totalSpent), id, rewardsReceiptStatus from receipts group by 2,3').fetchall()

#confirm that each receipt has one type of rewards status
conn.execute('''
             SELECT id, COUNT(DISTINCT rewardsReceiptStatus) as statusCount
             FROM receipts
             GROUP BY id
             HAVING COUNT(DISTINCT rewardsReceiptStatus) > 1''').fetchall()

## returns empty list, confirming assumption

#create table aggregated by ID
idAggQuery = '''
-- first aggregate by receipts id, as that will get you the amount spent by receipt.
CREATE TEMP TABLE ID_AGG AS 
SELECT avg(totalSpent) as totalSpendId, id, rewardsReceiptStatus 
FROM receipts 
group by 2,3;
'''
conn.execute(idAggQuery)

#Next, aggregate by rewards status
avgSpendSqlQuery = '''
--Next, aggregate by rewards status
SELECT rewardsReceiptStatus, round(avg(totalSpendId), 2) as totalSpentStatus
FROM ID_AGG
GROUP BY 1 
'''

conn.execute(avgSpendSqlQuery).fetchall()

"""
| rewardsReceiptStatus | totalSpentStatus |
|----------------------|------------------|
| FINISHED             | 81.17            |
| FLAGGED              | 180.45           |
| PENDING              | 28.03            |
| REJECTED             | 24.36            |
"""



# Looking at the above table, we can see that the highest average spend by the 'finished' receipt status is  $81.17, 
# which is much greater than the average spend by the 'rejected' receipt status of $24.36
# NOTE: I understand that the question asks for 'Accepted' rewardReceiptStatus, but that does not appear to be a value


### 2. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, 
###    which is greater?

#first, take a look at what the items of a specific receipt looks like
conn.execute('''
    SELECT id, purchasedItemCount, itemNumber, deleted, originalReceiptItemText
    FROM receipts
    WHERE id = '5f9c74f70a7214ad07000037'
''').fetchall()

"""
| id                      | purchasedItemCount | itemNumber | deleted | originalReceiptItemText |
|-------------------------|--------------------|------------|---------|-------------------------|
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
| 5f9c74f70a7214ad07000037| 11.0               | None       | None    | None                    |
"""


## Looking at the output, we can aggregate purchasedItemCount in a similar manner to how we aggregated totalSpent
#create table aggregated by ID
idCountAggQuery = '''
-- first aggregate by receipts id, as that will get you the amount spent by receipt.
CREATE TEMP TABLE ID_ITEM_AGG AS 
SELECT avg(purchasedItemCount) as avgItemCount, id, rewardsReceiptStatus 
FROM receipts 
group by 2,3;
'''
conn.execute(idCountAggQuery)

#Next, aggregate count by rewards status
avgCountSqlQuery = '''
--Next, aggregate by rewards status
SELECT rewardsReceiptStatus, round(avg(avgItemCount), 2) as totalSpentStatus
FROM ID_ITEM_AGG
GROUP BY 1 
'''

conn.execute(avgCountSqlQuery).fetchall()

"""
| rewardsReceiptStatus | totalSpentStatus |
|----------------------|------------------|
| FINISHED             | 15.86            |
| FLAGGED              | 22.04            |
| PENDING              | None             |
| REJECTED             | 2.54             |
"""

## Looking at the table above it appears as though the average receipt with status FINISHED ('Accepted') has approximately 16 items (15.86)
## whereas the average receipt with status 'REJECTED' has approximately 3 items 