import pandas as pd

# --- 1. Load Data ---
df_existing = pd.read_csv("final_sequential_sync.csv")
df_tbl = pd.read_csv("final_combined_data_raw_etch.csv")

# --- 2. Pre-process ---
df_existing['timestamp_etcher'] = pd.to_datetime(df_existing['timestamp_etcher'])
df_tbl['Created'] = pd.to_datetime(df_tbl['Created'])

def expand_merge_6h(left_df, right_df, left_ts, right_ts):
    """
    Expands the dataframe so that every match in the 6h window gets its own row.
    Uses left join to preserve all records from left_df.
    """
    # Create 6-hour buckets to prevent memory crash
    bucket_size = "6h"
    left_df['merge_key'] = left_df[left_ts].dt.floor(bucket_size)
    right_df['merge_key'] = right_df[right_ts].dt.floor(bucket_size)

    # Since a window can span across bucket boundaries, 
    # we allow matches in the current and the previous bucket
    right_shifted = right_df.copy()
    right_shifted['merge_key'] = right_shifted['merge_key'] - pd.Timedelta(hours=6)
    
    right_pool = pd.concat([right_df, right_shifted]).drop_duplicates()

    print("Performing expanded merge (preserving all LOT records)...")
    # Use left join to preserve all records from left_df
    merged = pd.merge(left_df, right_pool, on='merge_key', how='left', suffixes=('', '_sync'))

    # Calculate the exact time gap where both timestamps exist
    mask_both_exist = merged[left_ts].notna() & merged[right_ts].notna()
    merged.loc[mask_both_exist, 'diff_seconds'] = (merged.loc[mask_both_exist, right_ts] - merged.loc[mask_both_exist, left_ts]).dt.total_seconds()
    
    # For records with timestamp matches, filter by time window
    time_mask = (merged['diff_seconds'] > 0) & (merged['diff_seconds'] <= 6 * 3600)
    
    # Keep all records: either they match the time window OR they have no sync data (preserve LOT-only records)
    final_mask = time_mask | merged[right_ts].isna()
    final = merged[final_mask].copy()

    # Cleanup temporary columns
    final = final.drop(columns=['merge_key'], errors='ignore')
    if 'diff_seconds' in final.columns:
        final = final.drop(columns=['diff_seconds'])
    
    return final

# --- 3. Execution ---
# Use left join to preserve all records from df_tbl (which contains LOT data)
df_final = expand_merge_6h(df_tbl, df_existing, 'Created', 'timestamp_etcher')

# If no matches were found, we still want to preserve the original LOT data
if df_final.empty:
    print("No timestamp matches found, preserving original LOT data without sync information")
    df_final = df_tbl.copy()
    # Add empty sync_data column for consistency
    df_final['sync_data'] = None

# --- 4. Final Formatting ---
# Move sync_data to the first column
cols = ['sync_data'] + [c for c in df_final.columns if c != 'sync_data']
df_final = df_final[cols]

# --- 5. Export ---
df_final.to_csv("final_data.csv", index=False)

print(f"\nProcessing Complete.")
print(f"Original LOT Records: {len(df_tbl)}")
print(f"Sync Records Available: {len(df_existing)}")
print(f"Final Records (preserving all LOTs): {len(df_final)}")
records_with_sync = df_final.dropna(subset=['sync_data']).shape[0] if 'sync_data' in df_final.columns else 0
print(f"Records with sync data: {records_with_sync}")
print(f"Records with LOT data only: {len(df_final) - records_with_sync}")
