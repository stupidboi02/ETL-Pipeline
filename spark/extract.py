from bs4 import BeautifulSoup
import requests
from pyspark.sql.types import *
from datetime import datetime
import os
import pandas
run_time = datetime.now().strftime('%d%m%y')

columns = ["url", "content"]

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

    df = pandas.DataFrame(data= data, columns= columns)    # write to local
    data_path = os.path.abspath(path='spark/data/')
    path_parquet =  data_path + "/" + classify + "/" + run_time + ".parquet"
    df.to_parquet(path_parquet)

if __name__ == "__main__":
    # .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    extract('https://play.google.com/store/games?device=phone', 'game_phone')
    extract('https://play.google.com/store/games?device=tablet', 'game_tablet')
        



