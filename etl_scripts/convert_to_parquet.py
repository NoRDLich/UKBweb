import pandas as pd
import glob
import os
import duckdb # 或者使用 pyarrow

# --- 配置 ---
CSV_SOURCE_DIR = "../data_source/" # 指向你的CSV源文件夹 (移除了不存在的 A/B/C/)
PARQUET_OUTPUT_DIR = "../parquet_data/"  # Parquet文件输出目录
CSV_FILE_PATTERN = "temp_pheno_batch_*.csv"

def convert_csv_to_parquet():
    if not os.path.exists(PARQUET_OUTPUT_DIR):
        os.makedirs(PARQUET_OUTPUT_DIR)
        print(f"创建目录: {PARQUET_OUTPUT_DIR}")

    csv_files = glob.glob(os.path.join(CSV_SOURCE_DIR, CSV_FILE_PATTERN))
    if not csv_files:
        print(f"在 {CSV_SOURCE_DIR} 未找到匹配 {CSV_FILE_PATTERN} 的CSV文件。请检查路径和模式。")
        return

    print(f"找到 {len(csv_files)} 个CSV文件准备转换...")

    for i, csv_file_path in enumerate(csv_files):
        file_name_without_ext = os.path.splitext(os.path.basename(csv_file_path))[0]
        parquet_file_path = os.path.join(PARQUET_OUTPUT_DIR, f"{file_name_without_ext}.parquet")

        try:
            print(f"({i+1}/{len(csv_files)}) 正在转换 {csv_file_path} 到 {parquet_file_path} ...")
            
            # 使用DuckDB读取CSV并写入Parquet (对于非常宽的表，DuckDB可能比Pandas内存效率更高)
            # 注意：DuckDB的read_csv_auto对于非常多的列可能需要调整参数，或确保CSV格式良好
            # 你也可以用 pandas:
            # df = pd.read_csv(csv_file_path)
            # df.to_parquet(parquet_file_path, engine='pyarrow') # 确保安装了 pyarrow
            
            # 使用DuckDB (需要安装 duckdb)
            con = duckdb.connect()
            # 如果CSV没有表头，且列数固定为50万，你可能需要指定列名或让DuckDB自动生成
            # DuckDB的read_csv可以处理大量列，但请测试其性能和内存使用
            # 尝试让DuckDB自动推断，如果不行，可能需要更复杂的读取策略
            con.execute(f"COPY (SELECT * FROM read_csv_auto('{csv_file_path}', ALL_VARCHAR=TRUE)) TO '{parquet_file_path}' (FORMAT PARQUET);")
            # ALL_VARCHAR=TRUE 可以避免类型推断问题，之后在查询时可以CAST类型
            # 对于50万列，read_csv_auto 的性能和内存占用需要关注。
            # 如果CSV文件巨大，或列类型复杂，可能需要分块处理或更细致的read_csv参数设置。
            con.close()

            print(f"成功转换: {parquet_file_path}")

        except Exception as e:
            print(f"转换 {csv_file_path} 失败: {e}")

if __name__ == "__main__":
    convert_csv_to_parquet()
    print("ETL过程完成。")