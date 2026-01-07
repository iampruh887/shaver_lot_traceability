import pandas as pd

# 1. Load your datasets
# df_a contains 'CB_MELT'
# df_b contains 'Heat Melt'
df_a = pd.read_csv("tbl_etching_batch.csv") 
df_b = pd.read_excel("cleaned_raw_data.xlsx")

# 2. Standardize the join keys to ensure they match correctly
df_a['CB_MELT'] = pd.to_numeric(df_a['CB_MELT'], errors='coerce')
df_b['Heat Melt'] = pd.to_numeric(df_b['Heat Melt'], errors='coerce')

# 3. Perform the Left Outer Merge to preserve all LOT data
# 'how=left' keeps ALL rows from df_b (cleaned_raw_data) even if no CB_MELT match
df = pd.merge(
    df_b,  # Put df_b (with LOT data) as left to preserve all records
    df_a, 
    left_on='Heat Melt',
    right_on='CB_MELT', 
    how='left'  # Changed from 'inner' to 'left' to preserve all LOT records
)

# 4. Clean up the resulting DataFrame
# Handle the case where CB_MELT might be NaN for unmatched records
df['Melt_ID'] = df['Heat Melt']  # Use Heat Melt as the primary ID
df = df.drop(columns=['Heat Melt'])
# Only drop CB_MELT if it exists (it might be NaN for unmatched records)
if 'CB_MELT' in df.columns:
    df = df.drop(columns=['CB_MELT'])

# Move key columns (Melt_ID and Date) to the front
cols = ['Melt_ID', 'Date'] + [c for c in df.columns if c not in ['Melt_ID', 'Date']]
df = df[cols]

# 5. Final Formatting
df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
df = df.sort_values(by='Date', ascending=False).reset_index(drop=True)

# 6. Save the result
df.to_csv("final_combined_data_raw_etch.csv", index=False)

print(f"Left merge complete. {len(df)} total records preserved (including unmatched LOTs).")
matched_records = df.dropna(subset=[col for col in df.columns if col.startswith('CB_') or 'etching' in col.lower()]).shape[0] if any('CB_' in col or 'etching' in col.lower() for col in df.columns) else 0
print(f"Records with etching data: {matched_records}")
print(f"Records with LOT data only: {len(df) - matched_records}")
print(df.head())
