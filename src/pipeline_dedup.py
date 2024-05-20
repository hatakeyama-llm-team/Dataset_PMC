
import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
# from clustering import clustering_sentence
# from gensim.models import KeyedVectors
import itertools
import os
# from google.cloud import storage


import numpy as np
# from tqdm import tqdm
# from sklearn.cluster import KMeans, MiniBatchKMeans

    # 1.データセットDL
    # 2.k-meansクラスタリング
    # 3.文章分類
    # 4.GCSアップロード    



# 1.データセットDL
def download_files(bucket_name,input_dir_path = '../data/input_clustered/'):
    
    os.makedirs(input_dir_path, exist_ok=True)
    download_bucket_with_transfer_manager(bucket_name,input_dir_path)
    return input_dir_path

# 2.k-meansクラスタリング
# 3.文章分類    
def clustering_sentence(input_dir_path , n_clusters = 1000):
    from gensim.models import KeyedVectors    
    from classify.Text2Vec import Text2Vec
    files_path = os.listdir(input_dir_path)
    sentences = []
    for path in files_path:
        if path != '.DS_Store':
            with open(input_dir_path+path, "r") as f:                
                sentences.append(f.readlines())  
    sentences = list(itertools.chain.from_iterable(sentences))
    
    # 2.k-meansクラスタリング
    print("clustering...")
    t2v = Text2Vec(model=KeyedVectors.load_word2vec_format('./../model/entity_vector/entity_vector.model.bin', binary=True),dim=200,)
    title_vecs = [t2v.text2vec(i) for i in tqdm(sentences)]
    title_vecs = np.array(title_vecs)
    kmeans = MiniBatchKMeans(n_clusters=n_clusters, random_state=1).fit(title_vecs)
    # 各データポイントが割り当てられたクラスタのインデックスを取得
    labels = kmeans.labels_
    # 各クラスタに含まれるデータポイントの数を計算
    cluster_counts = dict((i, list(labels).count(i)) for i in range(n_clusters))
    print(labels)
    # 3.文章分類
    cluster_dict = {}
    for label,value in zip(labels,sentences):
        cluster_dict.setdefault(label, []).append(value)
    return cluster_dict   
# 4.GCSアップロード     
def upload_files(param,bucket_name = 'dataflow-test-ok'):
    from google.cloud import storage
    print(len(param))
    for key,value in param.items():        
        value = ''.join(value)
        upload_blob_from_memory(bucket_name,value,f"PMC_clustering_{key}.jsonl")
        
# GCPへ変数の値をアップロードする関数
def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents)
# GCPから複数ファイルをダウンロードする関数
def download_bucket_with_transfer_manager(bucket_name, destination_directory="", workers=2, max_results=2):
    from google.cloud.storage import Client, transfer_manager
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob_names = [blob.name for blob in bucket.list_blobs(max_results=max_results)]
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )
# GCPから単一ファイルをダウンロードする関数
def download_file(source_blob_name):
    from google.cloud import storage
    bucket_name = 'dataflow-test-ok'
    destination_file_name = f"./data/dedup_categorized/{source_blob_name}"
    directory = os.path.dirname(destination_file_name)
    print(source_blob_name)
    # ディレクトリが存在するか確認し、存在しない場合は作成する
    if not os.path.exists(directory):
        os.makedirs(directory)
    source_file_name = f'./output/dedup_{source_blob_name}'
    directory = os.path.dirname(source_file_name)
    # ディレクトリが存在するか確認し、存在しない場合は作成する    
    if not os.path.exists(directory):
        os.makedirs(directory)    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{source_blob_name}")
    blob.download_to_filename(destination_file_name)
    
    return source_blob_name
def run_command(job_name):
    input_dir = "./data/categorized"
    cmd = f'./dedup/deduplicate ./data/dedup_categorized/{job_name}'
    os.system(cmd)
    return job_name

def upload_blob(source_file_name):
    # param_input_list = os.listdir( './output/')
    # print(param_input_list)
    from google.cloud import storage
    bucket_name = 'dataflow-test-ok'
    source_file = f'./output/dedup_{source_file_name}'
    directory = os.path.dirname(source_file)
    # ディレクトリが存在するか確認し、存在しない場合は作成する    
    if not os.path.exists(directory):
        os.makedirs(directory)
    print(source_file)
    destination_blob_name = f'dedup_output/{source_file_name}'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0
    blob.upload_from_filename(source_file, if_generation_match=generation_match_precondition)



def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../sec/keen-hangar-419010-c00b94eccc4c-1.json"
    # bucket_name='pmc-clustering-sentences'
    options = StandardOptions()  # 1. 実行オプションの設定
    bucket_name ='dataflow-test-ok'
    options.runner = "DataflowRunner"  # Runnerもここで決めている #DataflowRunner
    google_cloud_options = options.view_as(GoogleCloudOptions)    
    job_name = f"genaic-dataflow-pmc-test-east1"
    google_cloud_options.region = "us-east1"
    google_cloud_options.project = "keen-hangar-419010"
    google_cloud_options.job_name = job_name + '1'
    google_cloud_options.staging_location = f"gs://{bucket_name}/stage"
    google_cloud_options.temp_location = f"gs://{bucket_name}/temp/"
    path_list = [f'PMC_clustering_{i}.jsonl' for i in range(10)]
    # upload_blob('./dedup/output/dedup_PMC_clustering_2.jsonl')
    with beam.Pipeline(options=options)  as p:
        file_name = (
        p | 'create file list' >> beam.Create(path_list)        
        #   | 'download' >> beam.Map(download_file)                
        #   | 'dedup' >> beam.Map(run_command)
        )
        a = file_name |'upload' >> beam.Map(upload_blob)
        

        
      



if __name__ == "__main__":
    main()