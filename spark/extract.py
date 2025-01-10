from bs4 import BeautifulSoup
import requests
from pyspark.sql.types import *
from datetime import datetime
import os
import pandas
from pyspark.sql import SparkSession


# ------------WRITE TO LOCAL----------------

# columns = ["url", "content"]
# def extract(url, classify):
#     request = requests.get(url).content # return the content of http request in BYTES form
#     soup = BeautifulSoup(request, 'html.parser')
#     t = soup.find_all('a')
#     data = []
#     for i in t:
#         if str(i).find('apps/details') != -1:
#             url = 'https://play.google.com' + i['href']
#             try:
#                 r = requests.get(url, timeout=3).content
#             except:
#                 TimeoutError
#             data.append((url, r.decode('utf-8'))) # decode - change data from BYTES to String 

#     df = pandas.DataFrame(data= data, columns= columns)    # write to local
#     data_path = os.path.abspath(path='spark/data/')
#     path_parquet =  data_path + "/" + classify + "/" + run_time + ".parquet"
#     df.to_parquet(path_parquet)

#-------------------WRITE TO HADOOP--------------------------
schema = StructType([
    StructField('url',dataType = StringType(), nullable=True),
    StructField('content',dataType=StringType(), nullable=True)
])
run_time = datetime.now().strftime('%d%m%y')
def extract(url, classify):
    request = requests.get(url).content # return the content of http request in BYTES form
    soup = BeautifulSoup(request, 'html.parser')
    t = soup.find_all('a')
    data = []
    for i in t:
        if str(i).find('apps/details') != -1:
            url = 'https://play.google.com' + i['href']
            try:
                r = requests.get(url, timeout=3).content
            except:
                TimeoutError
            data.append((url, r.decode('utf-8'))) # decode - change data from BYTES to String 

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.mode('overwrite').parquet(f'hdfs://namenode:9000/{classify}/{run_time}')

if __name__ == "__main__":
    spark = SparkSession.Builder()\
        .appName('extractToHDFS')\
        .getOrCreate()
    extract('https://play.google.com/store/games?device=phone', 'game_phone')
    extract('https://play.google.com/store/games?device=tablet', 'game_tablet')
        



