from bs4 import BeautifulSoup
import requests
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

schema = StructType([
    StructField('url',dataType = StringType(), nullable=True),
    StructField('content',dataType=StringType(), nullable=True)
])
run_time = datetime.now().strftime('%d%m%y')

def extract(url, classify, device, spark):
    request = requests.get(url).content # return the content of http request in BYTES form
    print(f"Crawling {url}")
    soup = BeautifulSoup(request, 'html.parser')
    data = []
    for i in soup.find_all('a'):
        if str(i).find('/store/apps/details') != -1:
            url = 'https://play.google.com' + i['href']
            try:
                r = requests.get(url, timeout=3).content
            except:
                TimeoutError
            data.append((url, r.decode('utf-8'))) # decode - change data from BYTES to String 

    df = spark.createDataFrame(data=data, schema=schema)
    df.write.mode('overwrite').parquet(f'hdfs://namenode:9000/{run_time}/{classify}/{device}')

if __name__ == "__main__":
    spark = SparkSession.Builder()\
        .appName('extractToHDFS')\
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    base_url = "https://play.google.com/store/"
    classify = ["games","apps"]
    devices = ["phone","tablet","tv"]
    for classi in classify:
        for devic in devices:
            url = base_url + classi + "?device=" + devic
            extract(url, classi, devic, spark)
        



