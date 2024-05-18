
# クリーニングしたwebテキストをクラスタリングするモデルを作る
# このフェーズでのクラスタリングの主目的は､dedupの計算コスト(N^2)を下げること｡
# dedup用に､n_clustersを大きくしておくのが吉
# 作成したファイルをGCSにUL
from classify.Text2Vec import Text2Vec
import joblib
import random
import numpy as np
from tqdm import tqdm
from gensim.models.fasttext import load_facebook_model
from sklearn.cluster import KMeans, MiniBatchKMeans
from load_gz import read_gzip_json_file, load_gzip_or_parquet
# from src.cleaner.auto_cleaner import clean_text
import json
from gensim.models import KeyedVectors
from datasets import load_dataset
import os

import itertools
def download_bucket_with_transfer_manager(bucket_name, destination_directory="", workers=2, max_results=2):
    from google.cloud.storage import Client, transfer_manager
    from clustering import clustering_sentence

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob_names = [blob.name for blob in bucket.list_blobs(max_results=max_results)]
    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )



def clustering_sentence(bucket_name='pmc-clean-sentences',input_dir_path = './input_clustering/',output_dir_path ='../output_clustering2/' , n_clusters = 1000):

    # 1.データセットDL
    # 2.k-meansクラスタリング
    # 3.文章分類
    # 4.GCSアップロード

    #1.データセットDL(HugginFace)
    # datasets = load_dataset(path="hatakeyama-llm-team/PMC",revision="main",data_files= 'PMC00*xxxxxx_0.jsonl')
    # sentences = datasets['train']    
    # sentences = [str(sentence) for sentence in sentences]
    
    # 1.データセットDL
    os.makedirs(input_dir_path, exist_ok=True)
    download_bucket_with_transfer_manager(bucket_name,input_dir_path)
    files_path = os.listdir(input_dir_path)
    sentences = []
    for path in tqdm(files_path):
        if path != '.DS_Store':
            with open(input_dir_path+path, "r") as f:                
                sentences.append(f.readlines())  
    sentences = list(itertools.chain.from_iterable(sentences))
    
    # # 2.k-meansクラスタリング
    # print("clustering...")
    # t2v = Text2Vec(model=KeyedVectors.load_word2vec_format('./../model/entity_vector/entity_vector.model.bin', binary=True),dim=200,)
    # title_vecs = [t2v.text2vec(i) for i in tqdm(param)]
    # title_vecs = np.array(title_vecs)
    # kmeans = MiniBatchKMeans(n_clusters=n_clusters, random_state=1).fit(title_vecs)
    # # 各データポイントが割り当てられたクラスタのインデックスを取得
    # labels = kmeans.labels_
    # # 各クラスタに含まれるデータポイントの数を計算
    # cluster_counts = dict((i, list(labels).count(i)) for i in range(n_clusters))
    
    # # 3.文章分類
    # cluster_dict = {}
    # for label,value in zip(labels,param):
    #     cluster_dict.setdefault(label, []).append(value)
    # return clustering_sentence
    # 4.GCSアップロード
    # ディレクトリを作成
    
    # os.makedirs(output_dir_path, exist_ok=True)
    # for key,value in cluster_dict.items():
    #     with open(output_dir_path+f"PMC_clustering_{key}.jsonl", "w") as f:
    #         f.write(''.join(value))
    # upload_file = os.listdir(output_dir_path) #[xxx.jsonl,xxx.jsonl]
    # upload_many_blobs_with_transfer_manager(bucket_name,upload_file,output_dir_path)





if __name__ == '__main__':
    clustering_sentence()


    
    # count_check = 0
    # for value in cluster_dict.values():
    #     count_check+= len(value)
    #     print(len(value))
    # print(count_check)




def upload_many_blobs_with_transfer_manager(bucket_name, filenames, source_directory="", workers=8):
    from google.cloud.storage import Client, transfer_manager
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    transfer_manager.upload_many_from_filenames(bucket, filenames, source_directory=source_directory, max_workers=workers)