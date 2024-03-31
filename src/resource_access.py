from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def save_analysis_results(sentences_data, file_selection='all', output_format='csv', output_dir='output'):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"{file_selection}_{timestamp}"
    output_path = Path(output_dir)

    # Modify the sentences_data to concatenate abstract and body with '\n\n'
    if output_format == 'parquet':
        for data in sentences_data:
            data['Text'] = data['Abstract'] + '\n\n' + data['Body']
            # Remove the now unnecessary keys to leave only what's needed for parquet
            del data['Abstract'], data['Body'], data['Abstract_Sentences_Count'], data['Body_Sentences_Count']

        # Convert list to DataFrame focusing on the new single column for parquet
        df = pd.DataFrame(sentences_data)[['File_Path', 'Text']]
    else:
        df = pd.DataFrame(sentences_data)

    if output_format == 'csv':
        csv_output_path = output_path / 'csv'
        csv_output_path.mkdir(parents=True, exist_ok=True)
        file_path = csv_output_path / f"{file_name}.csv"
        df.to_csv(file_path, index=False)
        print(f"Results saved to {file_path}")

    elif output_format == 'parquet':
        # Adjustments for saving in parquet format with a single 'Text' column
        parquet_output_path = output_path / 'parquet'
        parquet_output_path.mkdir(parents=True, exist_ok=True)
        file_path = parquet_output_path / f"{file_name}.parquet"

        # Since we've now structured the data differently for parquet, ensure it's saved appropriately
        df_parquet = pd.DataFrame(df['Text'].values, columns=['Text'])
        table = pa.Table.from_pandas(df_parquet)
        pq.write_table(table, file_path)
        print(f"Results saved to {file_path}")
