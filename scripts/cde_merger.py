import pandas as pd

# --- 1. Load Data ---
df_cl = pd.read_csv("CL_Cleaner.csv")
df_dv = pd.read_csv("CL_Developer.csv")
df_et = pd.read_csv("CL_Etcher4.csv")

# Rename and convert to datetime
df_cl = df_cl.rename(columns={'TimeStamp': 'timestamp_cleaner'})
df_dv = df_dv.rename(columns={'TimeStamp': 'timestamp_developer'})
df_et = df_et.rename(columns={'TimeStamp': 'timestamp_etcher'})

for df, col in zip([df_cl, df_dv, df_et], ['timestamp_cleaner', 'timestamp_developer', 'timestamp_etcher']):
    df[col] = pd.to_datetime(df[col])
    df.sort_values(col, inplace=True)

def merge_sequential_farthest(left_df, right_df, left_col, right_col, suffix):
    """
    Finds the farthest record that occurs AFTER the previous step 
    but within a 1-minute window. Uses left join to preserve all left records.
    """
    left_df['bin'] = left_df[left_col].dt.floor('1min')
    right_df['bin'] = right_df[right_col].dt.floor('1min')

    # Since it's sequential, we only need the current bin and the NEXT bin
    right_shifted = right_df.copy().assign(bin=right_df['bin'] - pd.Timedelta(minutes=1))
    right_pool = pd.concat([right_df, right_shifted])

    # Use left join to preserve all records from left_df
    merged = pd.merge(left_df, right_pool, on='bin', how='left', suffixes=('', suffix))
    
    # For records that have matches, apply the timing filter
    has_match = merged[right_col].notna()
    
    if has_match.any():
        # CRITICAL: Only keep rows where the next process happened AFTER the current one
        merged.loc[has_match, 'diff_seconds'] = (merged.loc[has_match, right_col] - merged.loc[has_match, left_col]).dt.total_seconds()
        
        # Filter: Must be between 0 and 60 seconds ahead (only for matched records)
        valid_timing = (merged['diff_seconds'] > 0) & (merged['diff_seconds'] <= 60)
        
        # Keep records that either have valid timing OR have no match (preserve unmatched)
        keep_mask = valid_timing | merged[right_col].isna()
        merged = merged[keep_mask]
        
        # For records with multiple valid matches, take the largest difference (farthest in the future)
        if 'diff_seconds' in merged.columns:
            merged = merged.sort_values('diff_seconds', ascending=False, na_position='last').drop_duplicates(subset=[left_col])
    
    # Cleanup
    columns_to_drop = ['bin']
    if 'diff_seconds' in merged.columns:
        columns_to_drop.append('diff_seconds')
    
    return merged.drop(columns=columns_to_drop, errors='ignore')

# --- 2. Execute Sequential Merge ---
print("Syncing: Cleaner -> Developer (Forward only, preserving all cleaner records)...")
df_final = merge_sequential_farthest(df_cl, df_dv, 'timestamp_cleaner', 'timestamp_developer', '_dv')
print(f"After Cleaner->Developer: {len(df_final)} records (from {len(df_cl)} cleaner records)")

print("Syncing: Developer -> Etcher (Forward only, preserving all records)...")
df_final = merge_sequential_farthest(df_final, df_et, 'timestamp_developer', 'timestamp_etcher', '_et')
print(f"After Developer->Etcher: {len(df_final)} records")

# --- 3. Calculate Sync Data Tuples ---
# Format: (Dev-Cl, Et-Dv) -> Both should now be positive seconds
diff_dv_cl = (df_final['timestamp_developer'] - df_final['timestamp_cleaner']).dt.total_seconds()
diff_et_dv = (df_final['timestamp_etcher'] - df_final['timestamp_developer']).dt.total_seconds()

df_final['sync_data'] = list(zip(diff_dv_cl.fillna('null'), diff_et_dv.fillna('null')))

# --- 4. Reorganize ---
cols = ['sync_data'] + [c for c in df_final.columns if c != 'sync_data']
df_final = df_final[cols]

# --- 5. Export ---
df_final.to_csv("final_sequential_sync.csv", index=False)
print(f"\nDone! All differences in sync_data should now be positive (Forward Time).")
print(f"Total records preserved: {len(df_final)}")
records_with_full_sync = df_final.dropna(subset=['timestamp_developer', 'timestamp_etcher']).shape[0]
print(f"Records with complete sync chain: {records_with_full_sync}")
print(f"Records with partial/no sync: {len(df_final) - records_with_full_sync}")