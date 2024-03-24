import warnings
warnings.simplefilter('ignore', FutureWarning)
# 以下のFutureWarningを無視するための設定
# /Dataset_PMC/.venv/lib/python3.10/site-packages/spacy/language.py:2195: FutureWarning: Possible set union at position 6328
#   deserializers["tokenizer"] = lambda p: self.tokenizer.from_disk(  # type: ignore[union-attr]

import argparse
from resource_access import save_analysis_results
from sentence_analysis import process_files_in_parallel
from pathlib import Path

def main(scope, output_format):
    input_dir = Path("./input")
    output_dir = Path("output")
    
    # inputディレクトリの1階層下のフォルダ名の一覧を取得
    subdirectories = [d for d in input_dir.iterdir() if d.is_dir()]
    print("Processing the following directories:")
    for subdirectory in subdirectories:
        print(subdirectory.name)

    # ディレクトリごとに実行
    for subdirectory in subdirectories:
        print(f"Processing started for {subdirectory.name}...")
        sentences_data = process_files_in_parallel(subdirectory, scope)
        # outputディレクトリをサブディレクトリ名に基づいて設定
        output_dir = f"output/{subdirectory.name}"
        save_analysis_results(sentences_data, file_selection=scope, output_format=output_format, output_dir=output_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process files in the given scope with optional output format.")
    parser.add_argument("scope", type=str, choices=['all', 'first100', 'last100'], help="Execution scope: all, first100, or last100.")
    parser.add_argument("output_format", type=str, choices=['csv', 'parquet'], help="Output format: csv or parquet.")
    args = parser.parse_args()
    main(args.scope, args.output_format)
