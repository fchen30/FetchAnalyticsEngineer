import json
import os

# Infer SQL data type from a Python value.
def infer_sql_type(value):
    if isinstance(value, int):
        return "INT"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, dict) and "$date" in value:
        return "TIMESTAMP"
    else:
        return "VARCHAR(500)"

# Generate SQL CREATE TABLE statements for a JSON file
def generate_create_tables(file_path, main_table_name):
    with open(file_path, "r", encoding="utf-8") as file:
        records = [json.loads(line) for line in file]

    # Collect all possible columns across all records, use set() to delete duplicates
    all_keys = set()
    nested_keys = {}
    # Find list and mark it as a nested field, store information in the list if it has key:value pairs
    for record in records:
        all_keys.update(record.keys())
        for key, value in record.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                if key not in nested_keys:
                    nested_keys[key] = set()
                for item in value:
                    nested_keys[key].update(item.keys())

    pk_name = f"{main_table_name}_id"

    # Columns for the main table
    main_columns = []
    # List of (name, columns) tuples for nested tables
    nested_tables = []

    for key in all_keys:
        if key == "_id":
            main_columns.append(f"{pk_name} VARCHAR(50) PRIMARY KEY")
        elif key in nested_keys:
            nested_table_name = f"{main_table_name}_{key}"
            nested_columns = [
                "item_id BIGINT IDENTITY(1,1) PRIMARY KEY",
                f"{pk_name} VARCHAR(50) REFERENCES {main_table_name}({pk_name})"
            ]
            for item_key in nested_keys[key]:
                sample_value = next((rec[key][0][item_key] for rec in records if
                                     key in rec and len(rec[key]) > 0 and item_key in rec[key][0]), None)
                nested_columns.append(f"{item_key} {infer_sql_type(sample_value)}")
            nested_tables.append((nested_table_name, nested_columns))
        else:
            sample_value = next((rec[key] for rec in records if key in rec), None)
            main_columns.append(f"{key} {infer_sql_type(sample_value)}")

    # Generate SQL for the main table
    create_main_table = f"CREATE TABLE IF NOT EXISTS {main_table_name} (\n    {', '.join(main_columns)}\n);"
    # Generate SQL for nested tables, if any
    create_nested_tables = [
        f"CREATE TABLE IF NOT EXISTS {name} (\n    {', '.join(cols)}\n);"
        for name, cols in nested_tables
    ]

    return create_main_table, create_nested_tables


# Define the three JSON files and their corresponding table names
file_table_pairs = [
    ("receipts.json", "receipts"),
    ("users.json", "users"),
    ("brands.json", "brands")
]

# Process each file and print the CREATE TABLE statements
for file_name, table_name in file_table_pairs:
    file_path = os.path.join(os.getcwd(), file_name)
    create_main, create_nested = generate_create_tables(file_path, table_name)
    print(create_main)
    for nested_table in create_nested:
        print(nested_table)


# Check if I can use barcode or brandcode as the join key
# File paths
receipts_file = os.path.join(os.getcwd(),"receipts.json")
brands_file = os.path.join(os.getcwd(), "brands.json")

# Load receipts data
with open(receipts_file, "r", encoding="utf-8") as file:
    receipts = [json.loads(line) for line in file]

# Load brands data
with open(brands_file, "r", encoding="utf-8") as file:
    brands = [json.loads(line) for line in file]

# Extract barcode and brandCode values from brands.json
brands_barcodes = set()
brands_codes = set()

for brand in brands:
    if "barcode" in brand:
        brands_barcodes.add(brand["barcode"])
    if "brandCode" in brand:
        brands_codes.add(brand["brandCode"])

# Extract barcode and brandCode values from receipts.json (inside rewardsReceiptItemList)
receipts_barcodes = set()
receipts_brand_codes = set()

for receipt in receipts:
    if "rewardsReceiptItemList" in receipt:
        for item in receipt["rewardsReceiptItemList"]:
            if "barcode" in item:
                receipts_barcodes.add(item["barcode"])
            if "brandCode" in item:
                receipts_brand_codes.add(item["brandCode"])

# Check matches
barcode_matches = receipts_barcodes.intersection(brands_barcodes)
brandcode_matches = receipts_brand_codes.intersection(brands_codes)

# Print results
print(f"Matching Barcodes: {len(barcode_matches)} / {len(receipts_barcodes)} in receipts, {len(receipts_barcodes)} unique value in receipts, {len(brands_barcodes)} unique value in brands")
print(f"Matching BrandCodes: {len(brandcode_matches)} / {len(receipts_brand_codes)} in receipts, {len(receipts_brand_codes)} nique value in receipts, {len(brands_codes)} unique value in brands")

#Check if brandcode and name has the same imput ignore letter case
all_match = all(
    str(brand.get("brandCode", "")).lower() == str(brand.get("name", "")).lower()
    for brand in brands
)
print("Do all brandCode values match name values (ignoring case)?", all_match)

matches = [
    brand for brand in brands
    if 'brandCode' in brand
    and brand['brandCode'].lower() == brand['name'].lower()
]
print(f"Matches: {len(matches)}/{len(brands)}")

#Check if barcode can be used as a join key
from collections import defaultdict
# Track barcode frequencies
barcode_counts = defaultdict(int)
missing_barcodes = 0

# Extract barcodes and count occurrences
for brand in brands:
    if 'barcode' in brand:
        barcode = brand['barcode']
        barcode_counts[barcode] += 1
    else:
        missing_barcodes += 1

# Identify duplicates
duplicates = {barcode: count for barcode, count in barcode_counts.items() if count > 1}

# Print results
total_barcodes = len(brands) - missing_barcodes
unique_barcodes = len(barcode_counts)

print(f"Total entries: {len(brands)}")
print(f"Missing barcodes: {missing_barcodes}")
print(f"Unique barcodes: {unique_barcodes}")
print(f"Duplicate barcodes: {len(duplicates)}")
if duplicates:
    print("\nDuplicate barcodes (barcode: count):")
    for barcode, count in duplicates.items():
        print(f"{barcode}: {count}")
else:
    print("\nAll barcodes are unique!")

#Check the receipt_item table
barcode_counts = defaultdict(int)
missing_barcodes = 0
total_items = 0
# Extract barcodes and count occurrences for Receipt
for receipt in receipts:
    if "rewardsReceiptItemList" in receipt:
        for item in receipt["rewardsReceiptItemList"]:
            total_items += 1
            if "barcode" in item:
                barcode = item['barcode']
                barcode_counts[barcode] += 1
            else:
                missing_barcodes += 1

# Identify duplicates
duplicates = {barcode: count for barcode, count in barcode_counts.items() if count > 1}

total_barcodes = len(receipts) - missing_barcodes
unique_barcodes = len(barcode_counts)

print(f"Total entries: {len(receipts)}")
print(f"Total items scanned: {total_items}")
print(f"Missing barcodes in items: {missing_barcodes}")
print(f"Unique barcodes: {unique_barcodes}")
print(f"Duplicate barcodes: {len(duplicates)}")
if duplicates:
    print("\nDuplicate barcodes (barcode: count):")
    for barcode, count in duplicates.items():
        print(f"{barcode}: {count}")
else:
    print("\nAll barcodes are unique!")
