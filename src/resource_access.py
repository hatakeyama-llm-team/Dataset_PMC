from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def save_analysis_results(sentences_data, file_selection='all', output_format='csv', output_dir='output'):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"{file_selection}_{timestamp}"
    output_path = Path(output_dir)

    # リストをDataFrameに変換
    df = pd.DataFrame(sentences_data, columns=['sentences_data'])  # カラム名を明示

    if output_format == 'csv':
        # 出力ディレクトリの設定
        csv_output_path = output_path / 'csv'
        csv_output_path.mkdir(parents=True, exist_ok=True)
        file_path = csv_output_path / f"{file_name}.csv"

        # DataFrameをCSVとして保存
        df.to_csv(file_path, index=False)
        print(f"Results saved to {file_path}")

    elif output_format == 'parquet':
        # 出力ディレクトリの設定
        parquet_output_path = output_path / 'parquet'
        parquet_output_path.mkdir(parents=True, exist_ok=True)
        file_path = parquet_output_path / f"{file_name}.parquet"

        # DataFrameをpyarrow.Tableに変換
        table = pa.Table.from_pandas(df)

        # TableをParquetとして保存
        pq.write_table(table, file_path)
        print(f"Results saved to {file_path}")
