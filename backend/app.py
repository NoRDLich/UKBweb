from flask import Flask, request, jsonify, render_template
import duckdb
import os
import glob
import re # For numerical sorting

app = Flask(__name__)

# --- 配置 ---
PARQUET_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'parquet_data'))

def natural_sort_key(s):
    """
    Key for natural sorting (e.g. "item1", "item2", "item10").
    Extracts numbers from a string and returns a list of strings and ints.
    """
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r'(\d+)', s)]

def get_available_parquet_files():
    """获取可用的Parquet文件名列表 (不含路径和扩展名), 并进行自然排序"""
    if not os.path.isdir(PARQUET_DATA_DIR):
        print(f"Error: Parquet data directory not found: {PARQUET_DATA_DIR}")
        return []
    
    parquet_files_paths = glob.glob(os.path.join(PARQUET_DATA_DIR, "temp_pheno_batch_*.parquet"))
    if not parquet_files_paths:
        print(f"Warning: No parquet files found in {PARQUET_DATA_DIR} matching pattern temp_pheno_batch_*.parquet")
        return []
        
    # 提取文件名，不含扩展名
    basenames = [os.path.splitext(os.path.basename(f))[0] for f in parquet_files_paths]
    
    # 使用自然排序
    basenames.sort(key=natural_sort_key)
    return basenames

@app.route('/')
def index():
    """渲染主页面"""
    return render_template('index.html')

@app.route('/api/files', methods=['GET'])
def list_files():
    """API接口，返回可用的Parquet文件名列表"""
    files = get_available_parquet_files()
    if not files:
        # It's possible the directory exists but is empty, or files don't match pattern.
        # Consider if this should be an error or just an empty list.
        # For now, if get_available_parquet_files prints a warning and returns [],
        # we'll return that empty list to the client.
        # If the directory itself is missing, get_available_parquet_files will print an error.
        pass # Let it return jsonify(files) which will be an empty list
    return jsonify(files)

@app.route('/api/get_data', methods=['POST'])
def get_data():
    data = request.get_json()
    if not data or 'selected_files' not in data:
        return jsonify({"error": "请选择文件"}), 400

    selected_basenames = data.get('selected_files', [])
    target_columns_str = data.get('target_columns', None) 

    if not selected_basenames:
        return jsonify({"error": "文件名列表为空"}), 400

    selected_file_paths = []
    for basename in selected_basenames:
        if not basename.startswith("temp_pheno_batch_") or ".." in basename or "/" in basename or "\\" in basename:
            return jsonify({"error": f"无效的文件名: {basename}"}), 400
        path = os.path.join(PARQUET_DATA_DIR, f"{basename}.parquet")
        if not os.path.exists(path):
             return jsonify({"error": f"文件未找到: {path}"}), 404
        selected_file_paths.append(path)
    
    if not selected_file_paths:
        return jsonify({"error": "没有有效的文件被选中或找到。"}), 404

    try:
        con = duckdb.connect(database=':memory:', read_only=False)
        
        # 逻辑：获取所有列名 (当 target_columns_str 是 GET_COLUMN_NAMES_ONLY)
        if target_columns_str == 'GET_COLUMN_NAMES_ONLY':
            all_unique_columns = set()
            # Create a separate connection for describing individual files to avoid interference
            # with the main connection 'con' that might be used for full_data_query later.
            # However, for just describing, using the same connection is fine.
            
            for file_path_for_cols in selected_file_paths:
                safe_file_path_for_cols = file_path_for_cols.replace('\\', '/')
                try:
                    # Describe columns for each file individually
                    cols_in_file_query = f"DESCRIBE SELECT * FROM read_parquet('{safe_file_path_for_cols}');"
                    cols_result = con.execute(cols_in_file_query).fetchall()
                    for row in cols_result: # row[0] is 'column_name'
                        all_unique_columns.add(row[0])
                except Exception as e_desc_file:
                    print(f"Warning: Could not describe columns for file {safe_file_path_for_cols}: {e_desc_file}")
                    # Optionally, you could decide to fail the request if any file can't be described
                    # return jsonify({"error": f"读取文件 {os.path.basename(safe_file_path_for_cols)} 的列结构时出错: {str(e_desc_file)}"}), 500
            
            if not all_unique_columns:
                 return jsonify({"error": "未能从选定文件中获取任何列名。"}), 500

            # Sort for consistent order to the frontend
            column_names = sorted(list(all_unique_columns))

            # --- Sample Data Logic (remains largely the same, but uses the main 'con' for full_data_query) ---
            # Build the UNION ALL query for sample data (if needed, or just return column names)
            union_sql_parts_sample = []
            for i, file_path in enumerate(selected_file_paths):
                safe_file_path = file_path.replace('\\', '/') 
                view_name = f"data_view_sample_{i}" # Use different view names if needed
                # Use the main 'con' for this, as it's related to the combined view
                con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS SELECT * FROM read_parquet('{safe_file_path}');")
                union_sql_parts_sample.append(f"SELECT * FROM {view_name}")
            
            if not union_sql_parts_sample:
                 return jsonify({"error": "未能为任何文件构建样本数据查询视图。"}), 500
            
            #full_data_query_sample = " UNION ALL ".join(union_sql_parts_sample)
            full_data_query_sample = " UNION ALL BY NAME ".join(union_sql_parts_sample) # 新代码

            sample_data_list = []
            sample_data_header = []
            sample_data_row_count = 0
            try:
                # Fetch one row of combined data to serve as a sample
                # The columns in this sample might not include *all* unique columns if that specific row doesn't have them
                sample_data_query_str = f"SELECT * FROM ({full_data_query_sample}) LIMIT 1;"
                sample_data_result = con.execute(sample_data_query_str)
                sample_data_header = [desc[0] for desc in sample_data_result.description] # Header from the actual query result
                sample_data_rows = sample_data_result.fetchall()
                sample_data_list = [dict(zip(sample_data_header, row)) for row in sample_data_rows]
                sample_data_row_count = len(sample_data_list)
            except Exception as e_sample:
                print(f"Warning: Could not fetch sample data: {e_sample}")
                # Not fatal, can still return column names

            con.close() # Close connection after use
            return jsonify({
                "all_columns": column_names, # This is the true union of all column names
                "sample_data_row_count": sample_data_row_count,
                "sample_data_columns": sample_data_header, # Columns present in the sample data
                "sample_data": sample_data_list
            })

        # --- Logic for fetching actual data with selected columns ---
        union_sql_parts_data = []
        for i, file_path in enumerate(selected_file_paths):
            safe_file_path = file_path.replace('\\', '/') 
            view_name = f"data_view_main_{i}"
            con.execute(f"CREATE OR REPLACE TEMP VIEW {view_name} AS SELECT * FROM read_parquet('{safe_file_path}');")
            union_sql_parts_data.append(f"SELECT * FROM {view_name}")
        
        if not union_sql_parts_data:
             return jsonify({"error": "未能为任何文件构建主数据查询视图。"}), 500

        #full_data_query_main = " UNION ALL ".join(union_sql_parts_data)
        full_data_query_main = " UNION ALL BY NAME ".join(union_sql_parts_data) # 新代码

        select_clause = "*"
        if target_columns_str and target_columns_str != '*':
            valid_columns = [col.strip() for col in target_columns_str.split(',') if col.strip()]
            if not valid_columns:
                 return jsonify({"error": "提供的目标列名无效或为空。"}), 400
            select_clause = ", ".join([f'"{col}"' for col in valid_columns]) # Quote column names

        # Add LIMIT for safety during development/testing, remove or make configurable for production
        # final_query_limit = " LIMIT 100" # Example limit
        final_query_limit = "" # No limit by default for actual data fetching, or make it a parameter

        final_query = f"SELECT {select_clause} FROM ({full_data_query_main}){final_query_limit};"
        
        query_result = con.execute(final_query)
        headers = [desc[0] for desc in query_result.description]
        data_rows = query_result.fetchall()
        results_list = [dict(zip(headers, row)) for row in data_rows]
        
        con.close()
        
        return jsonify({
            "columns": headers,
            "data": results_list,
            "row_count": len(results_list) 
        })

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        if 'con' in locals() and con: # Ensure connection is closed if it was opened
            con.close()
        return jsonify({"error": f"处理数据时发生错误: {str(e)}"}), 500

if __name__ == '__main__':
    print(f"Parquet 数据目录: {PARQUET_DATA_DIR}")
    # Test the file listing and sorting on startup
    print(f"可用的Parquet文件 (排序后): {get_available_parquet_files()}") 
    app.run(debug=True, port=5050)