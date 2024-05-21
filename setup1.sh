#畠山先生のベースコード参考
#https://github.com/hatakeyama-llm-team/Dataset_for_BTM/blob/main/setup.sh

conda create -n textprocess -y
conda activate textprocess
conda install pip -y

apt-get update -y 

# dataflow
pip install apache_beam
pip install 'apache-beam[gcp]'

# #gcs
pip install google-cloud-storage

apt install g++
apt install wget

apt install nlohmann-json3-dev -y

# git clone https://github.com/if001/dedup_sentence
# cd dedup_sentence
# git clone https://github.com/aappleby/smhasher.git
# wget https://raw.githubusercontent.com/simdjson/simdjson/master/singleheader/simdjson.h 
# wget https://raw.githubusercontent.com/simdjson/simdjson/master/singleheader/simdjson.cpp 
cd src/dedup
git clone https://github.com/aappleby/smhasher.git
git clone https://github.com/Tencent/rapidjson/
wget https://raw.githubusercontent.com/simdjson/simdjson/master/singleheader/simdjson.h 
wget https://raw.githubusercontent.com/simdjson/simdjson/master/singleheader/simdjson.cpp
g++ -o deduplicate deduplicate.cpp smhasher/src/MurmurHash3.cpp -I./smhasher/src -I./rapidjson/include/rapidjson -std=c++17

cd ~
#
#xcode-select --install
#brew install nlohmann-json


make