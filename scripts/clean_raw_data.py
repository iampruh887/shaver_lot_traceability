import pandas as pd

# --- 1. Load Data ---
df = pd.read_excel("raw_data.xlsx")

# --- 2. Extract Metadata & Initial Drops ---
supplier_name = df.iloc[1, 1] 

# Drop metadata rows and columns early to reduce memory usage
df = df.drop("Material - supplier CoA", axis=1)
df = df.drop(df.columns[26:105], axis=1) # Drop wide range of Unnamed columns
df = df.drop(df.index[[0, 1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]])

# --- 3. Header Repair (Merging Rows 2 and 3) ---
# We align the labels before promoting them to the header
row2 = df.loc[2].values.copy()
row3 = df.loc[3].values.copy()
start_col = 7

row2[start_col:] = row3[start_col:]
df.loc[2] = row2

# Drop the redundant Row 3 and the empty column at index 6
df = df.drop(index=3)
df = df.drop(df.columns[6], axis=1)

# --- 4. Column Label Refinement (Min/Max Suffixes) ---
# Loop through the Unnamed pairs to create descriptive headers
for i in range(15, 26, 2):
    col_current = f'Unnamed: {i}'
    col_next = f'Unnamed: {i+1}'
    
    base_label = df.at[2, col_current]
    df.at[2, col_current] = f"{base_label}-min"
    df.at[2, col_next] = f"{base_label}-max"

# Set the refined Row 2 as the actual DataFrame columns
df.columns = df.loc[2]
df = df.drop(index=2) # Drop the row used as header
df = df.reset_index(drop=True)

# --- 5. Data Cleaning & Filtering ---
# Step A: Standardize Date column
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

# Step B: Standardize Numeric columns
df['Heat Melt'] = pd.to_numeric(df['Heat Melt'], errors='coerce')

# Step C: Remove invalid rows (German phrases, empty rows, or only-date rows)
# 1. Drop rows where Date conversion failed (the text phrases)
df = df.dropna(subset=['Date'])

# 2. Drop rows where 'Heat Melt' is non-numeric or missing
df = df.dropna(subset=['Heat Melt'])

# 3. Drop rows where 'Heat Melt' AND 'LOT A' are both missing (extra check)
df = df.dropna(subset=['Heat Melt', 'LOT A'], how='all')

# 4. Final sweep for completely empty rows
df = df.dropna(how='all')

# --- 6. Final Formatting & Export ---
df['Date'] = df['Date'].dt.date
df = df.reset_index(drop=True)

print(f"Cleaned data for Supplier: {supplier_name}")
print(df.head())

df.to_excel("cleaned_raw_data.xlsx", index=False)