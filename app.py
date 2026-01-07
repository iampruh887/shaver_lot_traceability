from flask import Flask, render_template, request, send_from_directory, jsonify, send_file
import os, subprocess, uuid, traceback
import pandas as pd
from werkzeug.utils import secure_filename

app = Flask(__name__)
BASE = os.path.dirname(os.path.abspath(__file__))
JOBS = os.path.join(BASE, "jobs")
SCRIPTS = os.path.join(BASE, "scripts")

# Configuration
ALLOWED_EXTENSIONS = {'xlsx', 'csv'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

os.makedirs(JOBS, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ---------------- HOME ----------------
@app.route("/")
def index():
    return render_template("index.html")

# ---------------- UPLOAD & PIPELINE ----------------
@app.route("/upload", methods=["POST"])
def upload():
    try:
        files = request.files.getlist("files")
        
        if not files or all(f.filename == '' for f in files):
            return jsonify({"error": "No files selected"}), 400
        
        # Validate file types
        for f in files:
            if not allowed_file(f.filename):
                return jsonify({"error": f"Invalid file type: {f.filename}. Only .xlsx and .csv files are allowed."}), 400
        
        job_id = str(uuid.uuid4())
        job_dir = os.path.join(JOBS, job_id)
        os.makedirs(job_dir, exist_ok=True)

        # Save files with secure names
        saved_files = []
        for f in files:
            if f.filename:
                filename = secure_filename(f.filename)
                filepath = os.path.join(job_dir, filename)
                f.save(filepath)
                saved_files.append(filename)

        if not saved_files:
            return jsonify({"error": "No valid files were uploaded"}), 400

        # Pipeline execution with better error handling
        pipeline = [
            "clean_raw_data.py",
            "merge_raw_w_etch.py", 
            "cde_merger.py",
            "tbl_merge.py"
        ]

        for i, script in enumerate(pipeline):
            try:
                result = subprocess.run(
                    ["python", os.path.join(SCRIPTS, script)],
                    cwd=job_dir,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout per script
                )
                
                if result.returncode != 0:
                    error_msg = f"Script {script} failed: {result.stderr}"
                    print(f"Pipeline error: {error_msg}")
                    print(f"Script stdout: {result.stdout}")
                    return jsonify({
                        "error": f"Pipeline failed at step {i+1}/{len(pipeline)}: {script}",
                        "details": result.stderr[:500]  # Limit error message length
                    }), 500
                else:
                    print(f"Script {script} completed successfully")
                    if result.stdout:
                        print(f"Script {script} output: {result.stdout}")
                    if result.stderr:
                        print(f"Script {script} warnings: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                return jsonify({"error": f"Script {script} timed out after 5 minutes"}), 500
            except Exception as e:
                return jsonify({"error": f"Failed to execute {script}: {str(e)}"}), 500

        # Verify final output exists
        final_file = os.path.join(job_dir, "final_data.csv")
        if not os.path.exists(final_file):
            return jsonify({"error": "Pipeline completed but final_data.csv was not generated"}), 500

        return jsonify({
            "job_id": job_id,
            "download": f"/download/{job_id}/final_data.csv",
            "files_processed": saved_files
        })
        
    except Exception as e:
        print(f"Upload error: {traceback.format_exc()}")
        return jsonify({"error": f"Server error: {str(e)}"}), 500

# ---------------- DOWNLOAD ----------------
@app.route("/download/<job_id>/<filename>")
def download(job_id, filename):
    try:
        # Security: Only allow downloading from job directories
        job_dir = os.path.join(JOBS, job_id)
        if not os.path.exists(job_dir):
            return jsonify({"error": "Job not found"}), 404
            
        file_path = os.path.join(job_dir, filename)
        if not os.path.exists(file_path):
            return jsonify({"error": "File not found"}), 404
            
        # Security: Ensure file is within job directory
        if not os.path.commonpath([job_dir, file_path]) == job_dir:
            return jsonify({"error": "Access denied"}), 403
            
        return send_from_directory(job_dir, filename, as_attachment=True)
    except Exception as e:
        return jsonify({"error": f"Download failed: {str(e)}"}), 500

# ---------------- LOT SEARCH ----------------
@app.route("/search", methods=["POST"])
def search():
    try:
        job_id = request.form.get("job_id")
        lot_a = request.form.get("lot_a", "").strip()
        lot_b = request.form.get("lot_b", "").strip()

        if not job_id:
            return jsonify({"error": "Job ID not provided"}), 400

        if not lot_a and not lot_b:
            return jsonify({"error": "Please provide at least one LOT (A or B) to search"}), 400

        final_csv_path = os.path.join(JOBS, job_id, "final_data.csv")

        if not os.path.exists(final_csv_path):
            return jsonify({"error": "Data not found. Please run the pipeline first."}), 404

        # Load and process data
        df = pd.read_csv(final_csv_path)
        print(f"Loaded CSV with {len(df)} rows and columns: {list(df.columns)}")

        # Normalize column names
        df.columns = df.columns.str.strip()

        # Check if required columns exist
        if "LOT A" not in df.columns and "LOT B" not in df.columns:
            print(f"Available columns: {list(df.columns)}")
            return jsonify({"error": "LOT columns not found in data"}), 400

        # Debug: Show original LOT values before normalization
        if "LOT A" in df.columns:
            print(f"Original LOT A values (first 10): {df['LOT A'].head(10).tolist()}")
            print(f"LOT A data types: {df['LOT A'].dtype}")
            print(f"Unique LOT A values (first 20): {sorted(df['LOT A'].dropna().unique())[:20]}")

        if "LOT B" in df.columns:
            print(f"Original LOT B values (first 10): {df['LOT B'].head(10).tolist()}")
            print(f"LOT B data types: {df['LOT B'].dtype}")

        # Normalize LOT columns if they exist
        if "LOT A" in df.columns:
            # Handle different data types more carefully
            df["LOT A"] = df["LOT A"].fillna("")  # Replace NaN with empty string
            df["LOT A"] = (
                df["LOT A"].astype(str)
                .str.strip()
                .str.replace(r"\.0+$", "", regex=True)  # Remove trailing .0 or .00
                .str.replace("nan", "", regex=False)    # Remove 'nan' strings
            )
            print(f"Normalized LOT A values (first 10): {df['LOT A'].head(10).tolist()}")

        if "LOT B" in df.columns:
            df["LOT B"] = df["LOT B"].fillna("")  # Replace NaN with empty string
            df["LOT B"] = (
                df["LOT B"].astype(str)
                .str.strip()
                .str.replace(r"\.0+$", "", regex=True)  # Remove trailing .0 or .00
                .str.replace("nan", "", regex=False)    # Remove 'nan' strings
            )
            print(f"Normalized LOT B values (first 10): {df['LOT B'].head(10).tolist()}")

        # Debug: Show what we're searching for
        print(f"Searching for - LOT A: '{lot_a}', LOT B: '{lot_b}'")

        # Apply filters
        original_count = len(df)
        filtered_df = df.copy()  # Work with a copy to preserve original for debugging
        
        if lot_a and "LOT A" in df.columns:
            # Try exact match first
            matches = filtered_df[filtered_df["LOT A"] == lot_a]
            print(f"Exact match for LOT A '{lot_a}': {len(matches)} records")
            
            # If no exact match, try case-insensitive
            if len(matches) == 0:
                matches = filtered_df[filtered_df["LOT A"].str.lower() == lot_a.lower()]
                print(f"Case-insensitive match for LOT A '{lot_a}': {len(matches)} records")
            
            # If still no match, try partial match
            if len(matches) == 0:
                matches = filtered_df[filtered_df["LOT A"].str.contains(lot_a, case=False, na=False)]
                print(f"Partial match for LOT A '{lot_a}': {len(matches)} records")
            
            filtered_df = matches

        if lot_b and "LOT B" in df.columns:
            # Try exact match first
            matches = filtered_df[filtered_df["LOT B"] == lot_b]
            print(f"Exact match for LOT B '{lot_b}': {len(matches)} records")
            
            # If no exact match, try case-insensitive
            if len(matches) == 0:
                matches = filtered_df[filtered_df["LOT B"].str.lower() == lot_b.lower()]
                print(f"Case-insensitive match for LOT B '{lot_b}': {len(matches)} records")
            
            # If still no match, try partial match
            if len(matches) == 0:
                matches = filtered_df[filtered_df["LOT B"].str.contains(lot_b, case=False, na=False)]
                print(f"Partial match for LOT B '{lot_b}': {len(matches)} records")
            
            filtered_df = matches

        print(f"Final filtered results: {len(filtered_df)} records")

        if filtered_df.empty:
            search_terms = []
            if lot_a: search_terms.append(f"LOT A: {lot_a}")
            if lot_b: search_terms.append(f"LOT B: {lot_b}")
            
            # Show available values for debugging
            available_lots = []
            if "LOT A" in df.columns:
                unique_a = sorted([str(x) for x in df["LOT A"].dropna().unique() if str(x) and str(x) != "nan"])[:10]
                available_lots.append(f"Available LOT A values: {unique_a}")
            if "LOT B" in df.columns:
                unique_b = sorted([str(x) for x in df["LOT B"].dropna().unique() if str(x) and str(x) != "nan"])[:10]
                available_lots.append(f"Available LOT B values: {unique_b}")
            
            print(f"No matches found. {' | '.join(available_lots)}")
            
            return jsonify({
                "error": f"No records found for {' and '.join(search_terms)}",
                "debug": available_lots
            }), 404

        # Save search result
        output_path = os.path.join(JOBS, job_id, "lot_trace_result.csv")
        filtered_df.to_csv(output_path, index=False)
        print(f"Saved {len(filtered_df)} records to lot_trace_result.csv")

        return send_file(
            output_path,
            as_attachment=True,
            download_name="lot_trace_result.csv",
            mimetype="text/csv"
        )
        
    except Exception as e:
        print(f"Search error: {traceback.format_exc()}")
        return jsonify({"error": f"Search failed: {str(e)}"}), 500


# ---------------- DEBUG ENDPOINT ----------------
@app.route("/debug/<job_id>")
def debug_data(job_id):
    try:
        final_csv_path = os.path.join(JOBS, job_id, "final_data.csv")
        
        if not os.path.exists(final_csv_path):
            return jsonify({"error": "final_data.csv not found"}), 404
            
        df = pd.read_csv(final_csv_path)
        
        # Get basic info
        info = {
            "total_rows": len(df),
            "columns": list(df.columns),
            "sample_data": df.head(5).to_dict('records')
        }
        
        # Get LOT A and LOT B info if they exist
        if "LOT A" in df.columns:
            lot_a_values = df["LOT A"].dropna().unique()[:20]  # First 20 unique values
            info["lot_a_sample"] = [str(x) for x in lot_a_values]
            info["lot_a_count"] = len(df["LOT A"].dropna().unique())
            
        if "LOT B" in df.columns:
            lot_b_values = df["LOT B"].dropna().unique()[:20]  # First 20 unique values
            info["lot_b_sample"] = [str(x) for x in lot_b_values]
            info["lot_b_count"] = len(df["LOT B"].dropna().unique())
            
        return jsonify(info)
        
    except Exception as e:
        return jsonify({"error": f"Debug failed: {str(e)}"}), 500

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(debug=True)

