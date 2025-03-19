import pandas as pd
import os
pd.set_option('display.max_columns', 10, 'display.width', 500)

file_names = {
    "brands": "brands.json",
    "receipts": "receipts.json",
    "users": "users.json"
}

# Construct full file paths using os.path.join
file_paths = {name: os.path.join(os.getcwd(), filename) for name, filename in file_names.items()}

# Load JSON files into Pandas DataFrames
dataframes = {}
for name, path in file_paths.items():
    if os.path.exists(path):  # Ensure file exists before loading
        dataframes[name] = pd.read_json(path, lines=True)
    else:
        print(f"Warning: {name} file not found at {path}")

# Extract receipts and handle nested 'rewardsReceiptItemList'

receipts_df = dataframes['receipts']

items_list = []
for index, row in receipts_df.iterrows():
    receipt_id = row['_id']
    if isinstance(row['rewardsReceiptItemList'], list):  # Ensure it's a valid list
        for item in row['rewardsReceiptItemList']:
            item['receipt_id'] = receipt_id  # Link each item to its receipt
            items_list.append(item)

# Create a new DataFrame for rewardsReceiptItemList
rewards_items_df = pd.DataFrame(items_list)
# Store it separately in the dictionary
dataframes['rewards_items'] = rewards_items_df

for key, value in dataframes.items():
    print(f"\n{key} DataFrame Info:")
    print(value.info())

# Display the first few rows of each DataFrame
for key, df in dataframes.items():
    print(f"\n{key} DataFrame Head:")
    print(df.head())

# Check for duplicate records
"""
Duplicate Records:
- Brands: 0
- Receipts: 0
- Users: 283
- Rewards_items: 0
"""
duplicates = {name: df.astype(str).duplicated().sum() for name, df in dataframes.items()}
print("Duplicate Records:")
for name, count in duplicates.items():
    print(f"- {name.capitalize()}: {count}")


def basic_data_checks(df, df_name):
    print(f"\n{'=' * 30} DATA QUALITY CHECKS FOR {df_name.capitalize()} {'=' * 30}")

    # Missing values
    missing_values = df.isnull().sum()
    print("\nColumns with missing value:")
    print(missing_values)
    print("\nMissing value pct")
    print((df.isnull().mean()) * 100)

    # Duplicates (based on '_id' for receipts)
    if '_id' in df.columns:
        print(f"\nTotal {df_name.capitalize()}: {len(df)}")
        print(f"\nUnique {df_name.capitalize()} IDs: {df['_id'].astype(str).nunique()}")
        print(f"\nDuplicate {df_name.capitalize()} IDs: {df['_id'].astype(str).duplicated().sum()}")

        # Count Duplicate IDs
        duplicate_ids_counts = df['_id'].astype(str).value_counts()
        print(f"\nDuplicate {df_name.capitalize()} IDs Counts:\n{duplicate_ids_counts[duplicate_ids_counts > 1]}")

        # Count NaN in IDs
        print(f"\nTotal NaN in {df_name.capitalize()} IDs \n{df['_id'].isna().sum()}")


################# Users  ##########################
# Check for Duplicate User IDs and Missing Value
"""
Columns with missing value:
_id              0
active           0
createdDate      0
lastLogin       62
role             0
signUpSource    48
state           56
dtype: int64

Missing value pct
_id              0.000000
active           0.000000
createdDate      0.000000
lastLogin       12.525253
role             0.000000
signUpSource     9.696970
state           11.313131
dtype: float64
Total Users: 495
Unique Users IDs: 212
Duplicate Users IDs: 283


Count Duplicate User IDs

Duplicate Users IDs: 283
{'$oid': '54943462e4b07e684157a532'}    20
{'$oid': '5fc961c3b8cfca11a077dd33'}    20
{'$oid': '5ff5d15aeb7c7d12096d91a2'}    18
{'$oid': '59c124bae4b0299e55b0f330'}    18
{'$oid': '5fa41775898c7a11a6bcef3e'}    18

Total NaN in Users IDs 
0

"""

basic_data_checks(dataframes['users'], 'users')

# Count Users by Role
"""
(role: constant value set to 'CONSUMER'?)
role
consumer       413
fetch-staff     82
Name: count, dtype: int64
"""
role_counts = dataframes['users']['role'].value_counts(dropna=False)
print("User Roles Distribution:")
print(role_counts)

# Count Active vs. Inactive Users
"""
active
True     494
False      1
"""
active_status_counts = dataframes['users']['active'].value_counts(dropna=False)
print("Active vs. Inactive Users:")
print(active_status_counts)

# Count Users by Sign-Up Source
"""
User Sign-Up Sources:
signUpSource
Email     443
NaN        48
Google      4
"""
signup_source_counts = dataframes['users']['signUpSource'].value_counts(dropna=False)
print("User Sign-Up Sources:")
print(signup_source_counts)

# Count NaN in createdDate
print(f"Total NaN in CreatedDate \n{dataframes['users']['createdDate'].isna().sum()}")

# Count Users by State
"""
User Distribution by State:
state
WI     396
NaN     56
NH      20
AL      12
OH       5
IL       3
KY       1
CO       1
SC       1
"""
state_counts = dataframes['users']['state'].value_counts(dropna=False)
print("User Distribution by State:")
print(state_counts)

################# Brands ##########################

# List of key columns to check for missing values and IDs
"""
Columns with missing value:
_id               0
barcode           0
category        155
categoryCode    650
cpg               0
name              0
topBrand        612
brandCode       234
dtype: int64

Missing value pct
_id              0.000000
barcode          0.000000
category        13.281919
categoryCode    55.698372
cpg              0.000000
name             0.000000
topBrand        52.442159
brandCode       20.051414
dtype: float64

Total Brands: 1167
Unique Brands IDs: 1167
Duplicate Brands IDs: 0
Duplicate Brands IDs Counts:
Series([], Name: count, dtype: int64)

Total NaN in Brands IDs 
0
"""
basic_data_checks(dataframes['brands'], 'brands')

# Check duplicate brand names and barcodes
"""
Duplicate Brand Name Examples:
                                        _id                              name              barcode                         brandCode  

848   {'$oid': '585a961fe4b03e62d1ce0e76'}                         Baken-Ets              511111701781                         BAKEN-ETS
574   {'$oid': '5d9d08d1a60b87376833e348'}                         Baken-Ets              511111605546                         BAKEN ETS
Different _id shares the same brand name and brand code. Since the Brands table does not have duplicated rows, the _id is used as a unique record id

Duplicate Barcodes:
 barcode
511111504788    2
511111305125    2
511111504139    2
511111204923    2
511111605058    2
511111004790    2
511111704140    2

Duplicate Barcodes Examples:
                                       _id                              name                   barcode                         brandCode  
140   {'$oid': '5c409ab4cd244a3539b84162'}                              alexa                 511111004790                       ALEXA 
740   {'$oid': '5cdacd63166eb33eb7ce0fa8'}                              Bitten Dressing       511111004790                       BITTEN  

Same barcode/item under different brands
"""
duplicate_names = dataframes['brands'].duplicated(subset=['name'], keep=False)
duplicate_barcodes = dataframes['brands'].duplicated(subset=['barcode'], keep=False)

print("Duplicate Brand Names:\n", dataframes['brands'][duplicate_names]['name'].value_counts())
print("\nDuplicate Brand Name Examples:\n", dataframes['brands'][duplicate_names][['_id', 'name', 'barcode', 'brandCode']].sort_values(by = 'name'))
print("\nDuplicate Barcodes:\n",dataframes['brands'][duplicate_barcodes]['barcode'].value_counts())
print("\nDuplicate Barcodes Examples:\n", dataframes['brands'][duplicate_barcodes][['_id', 'name', 'barcode', 'brandCode']].sort_values(by = 'barcode'))

# Validate brandCode-name pairs
"""
Brands with multiple brandCodes:
                                  name  nunique                                             unique
73                          Baken-Ets        2                             [BAKEN ETS, BAKEN-ETS]
129                      Caleb's Kola        2                        [CALEB'S KOLA, CALEBS KOLA]
223               Dippin Dots® Cereal        2                  [DIPPIN DOTS, DIPPIN DOTS CEREAL]
313                   Health Magazine        2                             [511111605058, HEALTH]
335  I CAN'T BELIEVE IT'S NOT BUTTER!        2  [I CAN'T BELIEVE IT'S NOT BUTTER!, I CAN'T BEL...
504                 ONE A DAY® WOMENS        2                  [511111805854, ONE A DAY® WOMENS]
564                          Pull-Ups        2                                [PULL UPS, PULLUPS]

brandCodes are not clean, it has typos and multiple varieties
"""
brand_code_groups = dataframes['brands'].groupby('name')['brandCode'].agg(['nunique', 'unique']).reset_index()
print("Brands with multiple brandCodes:\n", brand_code_groups[brand_code_groups['nunique'] > 1])

# Check barcodes that is in more than one brand
"""
Barcodes used by multiple brands:
        barcode                                      name
0  511111004790                  [alexa, Bitten Dressing]
1  511111204923                       [Brand1, CHESTER'S]
2  511111305125  [Chris Image Test, Rachael Ray Everyday]
3  511111504139                   [Chris Brand XYZ, Pace]
4  511111504788                 [test, The Pioneer Woman]

What happened? Brands changed names or dirty data?
"""
barcode_groups = dataframes['brands'][duplicate_barcodes].groupby('barcode')['name'].apply(list).reset_index()
print("\n Barcodes used by multiple brands:")
print(barcode_groups)

# Check unique categories in brands dataset
"""
'Beauty & Personal Care'"' vs. 'Beauty' (merge?)
'Dairy & Refrigerated' vs. 'Dairy'( merge?)
'Cleaning & Home Improvement' vs.  'Household' (merge?)
"""
unique_categories = dataframes['brands']['category'].dropna().unique()
print("Unique Categories in Brands:")
print(pd.Series(unique_categories).sort_values())

# Check unique category Codes in brands dataset
"""
Unique Category Codes:
0                              BABY
1                            BAKING
2                 BEER_WINE_SPIRITS
3                         BEVERAGES
4                  BREAD_AND_BAKERY
5                  CANDY_AND_SWEETS
6     CLEANING_AND_HOME_IMPROVEMENT
7            DAIRY_AND_REFRIGERATED
8                            FROZEN
9                           GROCERY
10             HEALTHY_AND_WELLNESS
11                        MAGAZINES
12                          OUTDOOR
13                    PERSONAL_CARE
"""
print("\nUnique Category Codes:\n", pd.Series(dataframes['brands']['categoryCode'].dropna().unique()).sort_values().reset_index(drop=True))

# Validate category-code pairs
"""
Category with multiple categoryCodes:
Empty DataFrame
"""
category_code_groups = dataframes['brands'].groupby('category')['categoryCode'].agg(['nunique', 'unique']).reset_index()
print("Category with multiple categoryCodes:\n", category_code_groups[category_code_groups['nunique'] > 1])

# Ensure "topBrand" only contains True/False
"""
Invalid 'topBrand' Values:
                                        _id                       name  topBrand
7     {'$oid': '5cdad0f5166eb33eb7ce0faa'}                 J.L. Kraft       NaN
9     {'$oid': '5c408e8bcd244a1fdb47aee7'}                       test       NaN
10    {'$oid': '5f4bf556be37ce0b4491554d'}  test brand @1598813526777       NaN
11    {'$oid': '57c08106e4b0718ff5fcb02c'}                MorningStar       NaN
13    {'$oid': '5d6413156d5f3b23d1bc790a'}       Entertainment Weekly       NaN
...
Should only has True or False in the topBrand column
"""
print("\nInvalid 'topBrand' Values:\n", dataframes['brands'][['_id', 'name', 'topBrand']][~dataframes['brands']['topBrand'].isin([True, False])])


################    Receipts    ################

# Basic Check
"""
Columns with missing value:
_id                          0
bonusPointsEarned          575
bonusPointsEarnedReason    575
createDate                   0
dateScanned                  0
finishedDate               551
modifyDate                   0
pointsAwardedDate          582
pointsEarned               510
purchaseDate               448
purchasedItemCount         484
rewardsReceiptItemList     440
rewardsReceiptStatus         0
totalSpent                 435
userId                       0
dtype: int64

Can I use creationDate where purchaseDate is empty?

Missing value pct
_id                         0.000000
bonusPointsEarned          51.385165
bonusPointsEarnedReason    51.385165
createDate                  0.000000
dateScanned                 0.000000
finishedDate               49.240393
modifyDate                  0.000000
pointsAwardedDate          52.010724
pointsEarned               45.576408
purchaseDate               40.035746
purchasedItemCount         43.252904
rewardsReceiptItemList     39.320822
rewardsReceiptStatus        0.000000
totalSpent                 38.873995
userId                      0.000000
dtype: float64

Total Receipts: 1119
Unique Receipts IDs: 1119
Duplicate Receipts IDs: 0
Duplicate Receipts IDs Counts:
Series([], Name: count, dtype: int64)

Total NaN in Receipts IDs 
0
"""
basic_data_checks(dataframes['receipts'], "receipts")

# Check valid receipt statuses
"""
Receipt statuses found: 
['FINISHED' 'REJECTED' 'FLAGGED' 'SUBMITTED' 'PENDING']
"""
print(f"Receipt statuses found: \n{dataframes['receipts']['rewardsReceiptStatus'].unique()}")

# Check missing rewardsReceiptItemList and totalSpent by rewardReceiptStatus
"""
                      rewardsReceiptItemList  totalSpent
rewardsReceiptStatus                                    
FINISHED                                   2           0
FLAGGED                                    0           0
PENDING                                    1           1
REJECTED                                   3           0
SUBMITTED                                434         434
"""
nan_counts = dataframes['receipts'].groupby('rewardsReceiptStatus').agg({
    'rewardsReceiptItemList': lambda x: x.isna().sum(),
    'totalSpent': lambda x: x.isna().sum()
})

print(nan_counts)

# check how many rewardsReceiptStatus per receipt
"""
 Any receipt with more than 1 review status: False
"""
status_counts = dataframes['receipts'].groupby('_id')['rewardsReceiptStatus'].nunique()
print(f"\n Any receipt with more than 1 review status: {status_counts[status_counts > 1].any()}")

# Check valid bonusPointsEarnedReason
"""
['Receipt number 2 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)'
 'Receipt number 5 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)'
 'All-receipts receipt bonus'
 'Receipt number 1 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)'
 'Receipt number 3 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)'
 'Receipt number 6 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)'
 'Receipt number 4 completed, bonus point schedule DEFAULT (5cefdcacf3693e0b50e83a36)'
 nan 'COMPLETE_PARTNER_RECEIPT' 'COMPLETE_NONPARTNER_RECEIPT']
"""
print(f"bonusPointsEarnedReason found: \n{dataframes['receipts']['bonusPointsEarnedReason'].unique()}")

# Check if NaN in bonusPointsEarnedReason means NaN in pointsEarned?
"""
Number of NaN rows in pointsEarned: 575, it is the same number of NaN rows in bonusPointsEarnedReason
NaN in bonusPointsEarnedReason means NaN in pointsEarned: True
"""
print(f"Number of NaN rows in pointsEarned: {dataframes['receipts'][dataframes['receipts']['bonusPointsEarnedReason'].isna()]['bonusPointsEarned'].isna().sum()}")
print(f"NaN in bonusPointsEarnedReason means NaN in pointsEarned: {dataframes['receipts'][dataframes['receipts']['bonusPointsEarnedReason'].isna()]['bonusPointsEarned'].isna().all()}")

# Check totalSpent is non-negative
def negative_value(df, column):
    print(f"Negative {column} values: {len(df[df[column].astype(float) < 0])}")
"""
Negative totalSpent values: 0
"""
negative_value(dataframes['receipts'],'totalSpent')

# Check purchasedItemCount is non-negative
"""
0
"""
negative_value(dataframes['receipts'],'purchasedItemCount')


# Check bonusPointsEarned is non-negative
"""
0
"""
negative_value(dataframes['receipts'],'bonusPointsEarned')


# Check pointsEarned is non-negative
"""
0
"""
negative_value(dataframes['receipts'],'pointsEarned')

# Check if purchaseDate and createDate is same
"""
unmatched examples: 
             purchaseDate          createDate
0    2021-01-03 00:00:00 2021-01-03 15:25:31
1    2021-01-02 15:24:43 2021-01-03 15:24:43
2    2021-01-03 00:00:00 2021-01-03 15:25:37
3    2021-01-03 00:00:00 2021-01-03 15:25:34
"""
dataframes['receipts']['purchaseDate'] = dataframes['receipts']['purchaseDate'].apply(
    lambda x: pd.to_datetime(x['$date'], unit='ms') if pd.notna(x) else pd.NaT
)
dataframes['receipts']['createDate'] = dataframes['receipts']['createDate'].apply(
    lambda x: pd.to_datetime(x['$date'], unit='ms') if pd.notna(x) else pd.NaT
)

filtered_na_df = dataframes['receipts'][dataframes['receipts']['purchaseDate'].notna()]
date_match = filtered_na_df['purchaseDate'] == filtered_na_df['createDate']
match_count = date_match.sum()
total_rows = len(filtered_na_df)
match_percentage = (match_count / total_rows) * 100

print(f"Matching dates: {match_count}/{total_rows} ({match_percentage:.2f}%)")
print(f"\nNon-matching dates: {total_rows - match_count}/{total_rows}")


# Purchase and Creation, which event came first?
"""
Purchase earlier dates: 658/671 (98.06%)
Purchase later dates: 13/671
"""
date_earlier = filtered_na_df['purchaseDate'] <= filtered_na_df['createDate']
earlier_count = date_earlier.sum()
earlier_percentage = (earlier_count / total_rows) * 100

print(f" Purchase earlier dates: {earlier_count}/{total_rows} ({earlier_percentage:.2f}%)")
print(f"\n Purchase later dates: {total_rows - earlier_count}/{total_rows}")

############### Rewards_items ##################

dataframes['rewards_items']['receipt_id'] = dataframes['rewards_items']['receipt_id'].apply(
    lambda x: x.get('$oid') if isinstance(x, dict) else None
)

# Basic Checks
"""
Columns with missing value:
barcode                               3851
description                            381
finalPrice                             174
itemPrice                              174
needsFetchReview                      6128
partnerItemId                            0
preventTargetGapPoints                6583
quantityPurchased                      174
userFlaggedBarcode                    6604
userFlaggedNewItem                    6618
userFlaggedPrice                      6642
userFlaggedQuantity                   6642
receipt_id                               0
needsFetchReviewReason                6722
pointsNotAwardedReason                6601
pointsPayerId                         5674
rewardsGroup                          5210
rewardsProductPartnerId               4672
userFlaggedDescription                6736
originalMetaBriteBarcode              6870
originalMetaBriteDescription          6931
brandCode                             4341
competitorRewardsGroup                6666
discountedItemPrice                   1172
originalReceiptItemText               1181
itemNumber                            6788
originalMetaBriteQuantityPurchased    6926
pointsEarned                          6014
targetPrice                           6563
competitiveProduct                    6296
originalFinalPrice                    6932
originalMetaBriteItemPrice            6932
deleted                               6932
priceAfterCoupon                      5985
metabriteCampaignId                   6078
_id                                      0
_id2                                     0
dtype: int64
Missing value pct
barcode                               55.481919
description                            5.489123
finalPrice                             2.506843
itemPrice                              2.506843
needsFetchReview                      88.286990
partnerItemId                          0.000000
preventTargetGapPoints                94.842242
quantityPurchased                      2.506843
userFlaggedBarcode                    95.144792
userFlaggedNewItem                    95.346492
userFlaggedPrice                      95.692263
userFlaggedQuantity                   95.692263
receipt_id                             0.000000
needsFetchReviewReason                96.844835
pointsNotAwardedReason                95.101570
pointsPayerId                         81.746146
rewardsGroup                          75.061230
rewardsProductPartnerId               67.310186
userFlaggedDescription                97.046535
originalMetaBriteBarcode              98.977093
originalMetaBriteDescription          99.855929
brandCode                             62.541421
competitorRewardsGroup                96.038035
discountedItemPrice                   16.885175
originalReceiptItemText               17.014839
itemNumber                            97.795707
originalMetaBriteQuantityPurchased    99.783893
pointsEarned                          86.644576
targetPrice                           94.554099
competitiveProduct                    90.707391
originalFinalPrice                    99.870336
originalMetaBriteItemPrice            99.870336
deleted                               99.870336
priceAfterCoupon                      86.226768
metabriteCampaignId                   87.566633
_id                                    0.000000
_id2                                   0.000000
dtype: float64
Total Rewards_items: 6941
Unique Rewards_items IDs: 2060
Duplicate Rewards_items IDs: 4881
Duplicate Rewards_items IDs Counts:
_id
600f2fc80a720f0535000030nan             303
600f39c30a7214ada2000030nan             298
600f24970a720f053500002fnan             286
600f0cc70a720f053500002cnan             176
600a1a8d0a7214ada2000008nan             163
                                       ... 
600f2fc80a720f0535000030511111704140      2
60145a540a720f05f8000116nan               2
601830cd0a720f05f800034fnan               2
60189c920a7214ad2800003anan               2
5ff1e1e90a7214ada1000569nan               2
Name: count, Length: 503, dtype: int64
Total NaN in Rewards_items IDs 
0
"""
# Assuming receipt_id + barcode can be used as the unique identifier
dataframes['rewards_items']['_id'] = dataframes['rewards_items']['receipt_id'].astype(str) + dataframes['rewards_items']['barcode'].astype(str)
basic_data_checks(dataframes['rewards_items'], "rewards_items")

# Check receipt with NaN barcodes
# {'$oid': '600f2fc80a720f0535000030'} as example
"""
NaN barcodes count under the receipt 600f2fc80a720f0535000030 :
303/459

This receipt didnt record most barcode data

Any duplicated rows? 
False
"""
example_df = dataframes['rewards_items'][dataframes['rewards_items']['receipt_id'].astype(str) == '600f2fc80a720f0535000030']
print('NaN barcodes count under the receipt 600f2fc80a720f0535000030 :')
print(f"{example_df['barcode'].isnull().sum()}/{len(example_df)}")
print('\n Total missing values')
print(example_df.isnull().sum())
print(f"\nAny duplicated rows? \n{example_df.astype(str).value_counts()[example_df.astype(str).value_counts() > 1].any()}")

# Here is the list of records with duplicated IDs
"""
5384 duplicated receipt_id+barcode
Any duplicated rows? 
False
Meaning receipt_id+barcode alone can not be used as the unique identifier, maybe use receipt_id + partnerItemID instead?
"""
duplicated_ids_df = dataframes['rewards_items'][dataframes['rewards_items'].groupby('_id')['_id'].transform('count') > 1]
print(duplicated_ids_df[['receipt_id','barcode']].head())
print(f"\n{len(duplicated_ids_df)} duplicated receipt_id+barcode")
print(f"\nAny duplicated rows? \n{duplicated_ids_df.astype(str).value_counts()[duplicated_ids_df.astype(str).value_counts() > 1].any()}")

"""
Any duplicated rows? 
False
receipt_id + partnerItemID is unique
"""
dataframes['rewards_items']['_id2'] = dataframes['rewards_items']['partnerItemId'].astype(str) + dataframes['rewards_items']['receipt_id'].astype(str)
print(f"\nAny duplicated rows? \n{dataframes['rewards_items']['_id2'].value_counts()[dataframes['rewards_items']['_id2'].value_counts()>1].any()}")
"""
                  receipt_id       barcode partnerItemId  quantityPurchased itemPrice finalPrice
14  5ff1e1b60a7214ada100055c  034100573065             1                1.0        29         29
15  5ff1e1b60a7214ada100055c  034100573065             2                1.0        29         29
16  5ff1e1b60a7214ada100055c  034100573065             3                1.0        29         29
17  5ff1e1b60a7214ada100055c  034100573065             4                1.0        29         29
18  5ff1e1b60a7214ada100055c  034100573065             5                1.0        29         29
19  5ff1e1b60a7214ada100055c  034100573065             6                1.0        29         29
20  5ff1e1b60a7214ada100055c  034100573065             7                1.0        29         29
21  5ff1e1b60a7214ada100055c  034100573065             8                1.0        29         29
22  5ff1e1b60a7214ada100055c  034100573065             9                1.0        29         29
23  5ff1e1b60a7214ada100055c  034100573065            10                1.0        29         29

Duplicated record? Or should user sum(quantityPurchased*itemPrice) group by receipt_id, barcode to get the totalSpent per item in one receipt?
"""
print(duplicated_ids_df[duplicated_ids_df['receipt_id']=='5ff1e1b60a7214ada100055c'][['receipt_id','barcode','partnerItemId','quantityPurchased','itemPrice', 'finalPrice']])

# Extract ID from dictionary
dataframes['receipts']['_id'] = dataframes['receipts']['_id'].apply(
    lambda x: x.get('$oid') if isinstance(x, dict) else None
)

"""
290.0 = 10 * finalPrice meaning we need to add up the quantityPurchased * finalPrice for the duplicated receipt_id + barcode 
"""
print(dataframes['receipts'][dataframes['receipts']['_id'] == '5ff1e1b60a7214ada100055c']['totalSpent'])

# Check for rows where itemPrice != finalPrice
"""
Total mismatched prices: 178
"""
mismatches = dataframes['rewards_items'][
    dataframes['rewards_items']['itemPrice'] != dataframes['rewards_items']['finalPrice']
]
print(f"Total mismatched prices: {len(mismatches)}")
"""
                    receipt_id partnerItemId itemPrice finalPrice  quantityPurchased discountedItemPrice
1825  600260210a720f05f300008f          1213      4.99       2.88                1.0                4.99
2264  60049d9d0a720f05f3000094          1371      3.59       2.89                1.0                2.89
2266  60049d9d0a720f05f3000094          1374      2.59       2.39                1.0                2.39
2267  60049d9d0a720f05f3000094          1376      2.99       2.50                1.0                2.50

Not all DiscountedItemPrices are used as finalPrices
"""
print(mismatches[['receipt_id', 'partnerItemId', 'itemPrice', 'finalPrice','quantityPurchased', 'discountedItemPrice']].dropna().head())

# Check if finalPrice is used to calculate totalSpent
"""
                    receipt_id partnerItemId itemPrice finalPrice  quantityPurchased
2264  60049d9d0a720f05f3000094          1371      3.59       2.89                1.0
2266  60049d9d0a720f05f3000094          1374      2.59       2.39                1.0
2267  60049d9d0a720f05f3000094          1376      2.99       2.50                1.0

The difference between the sum(itemPrice) and sum(finalPrice) is 1.39

For receipt 60049d9d0a720f05f3000094: 
 totalSpent: 743.79
 Calculated totalSpent finalPrice: 833.37
 Calculated totalSpent itemPrice: 834.7600000000001
 
 834.76 - 833.37 = 1.39
 
The difference is the same, but still do not know why the totalSpent is different from the calculated_Total
"""
print(mismatches.loc[mismatches['receipt_id'] == '60049d9d0a720f05f3000094', ['receipt_id', 'partnerItemId', 'itemPrice', 'finalPrice','quantityPurchased']])

rewards_items_df = dataframes['rewards_items'][dataframes['rewards_items']['receipt_id'] == '60049d9d0a720f05f3000094'].copy()
rewards_items_df['finalPrice'] = rewards_items_df['finalPrice'].astype(float)
rewards_items_df['itemPrice'] = rewards_items_df['itemPrice'].astype(float)
rewards_items_df['quantityPurchased'] = rewards_items_df['quantityPurchased'].astype(float)
item_totals_fp = rewards_items_df.groupby('receipt_id').apply(lambda x: (x['finalPrice'] * x['quantityPurchased']).sum()).reset_index(name='calculated_total')
item_totals_ip = rewards_items_df.groupby('receipt_id').apply(lambda x: (x['itemPrice'] * x['quantityPurchased']).sum()).reset_index(name='calculated_total')

print(f"\n totalSpent: {dataframes['receipts'][dataframes['receipts']['_id'] == '60049d9d0a720f05f3000094']['totalSpent'].values[0]}")
print(f"\n Calculated totalSpent finalPrice: {item_totals_fp['calculated_total'].values[0]}")
print(f"\n Calculated totalSpent itemPrice: {item_totals_ip['calculated_total'].values[0]}")


#Check Negative Values
"""
Negative itemPrice values: 0
Negative finalPrice values: 0
Negative quantityPurchased values: 0

"""
negative_value(dataframes['rewards_items'],'itemPrice')
negative_value(dataframes['rewards_items'],'finalPrice')
negative_value(dataframes['rewards_items'],'quantityPurchased')

# Check if ALL Reviews have a reason
"""
Missing Review Reasons: 0
"""
incorrect_flags = dataframes['rewards_items'][
    (dataframes['rewards_items']['needsFetchReview'] == True) &
    (dataframes['rewards_items']['needsFetchReviewReason'].isna())
].shape[0]
print(f"Missing Review Reasons: {incorrect_flags}")



