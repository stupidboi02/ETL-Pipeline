from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
from datetime import datetime
from bs4 import BeautifulSoup
import re

schema = StructType([StructField("name", StringType(), True),
    StructField("company", StringType(), True),
    StructField("ratings", DoubleType(), True),
    StructField("reviews", DoubleType(), True),
    StructField("age", StringType(), True),
    StructField("downloads", DoubleType(), True),
    StructField("classify", StringType(), True),
    StructField("describe", StringType(), True),
    StructField("lastVersion", StringType(), True),
])

schema_hdfs = StructType([
    StructField('url', StringType(), True),
    StructField('content', StringType(), True),])

@udf(returnType=StringType())
def extract_name(content):
    soup = BeautifulSoup(content, 'html.parser')
    names = soup.find_all('h1')
    for name in names:
        if name.find('span', class_='AfwdI', itemprop='name'):
            return name.text
    return None

@udf(returnType=StringType())
def extract_company(content):
    soup = BeautifulSoup(content, 'html.parser')
    company = soup.find_all(class_='Vbfug auoIOc')
    return company[0].text if company else None
   
@udf(returnType=DoubleType())
def extract_rating(content):
    soup = BeautifulSoup(content, 'html.parser')
    ratings = soup.find_all(class_='jILTFe')
    if ratings:
        try:
            return float(ratings[0].text)
        except ValueError:
            return 0.0
    return 0.0

@udf(returnType=DoubleType())
def extract_number_of_review(content):
    soup = BeautifulSoup(content, 'html.parser')
    reviewAndAges = soup.find_all(class_='g1rdde')
    if reviewAndAges:
        reviews = reviewAndAges[0].text.strip().split(' ')[0]
        try:
            if 'K' in reviews:
                return float(reviews.replace('K', '')) * 1000.0
            elif 'M' in reviews:
                return float(reviews.replace('M', '')) * 1000000.0
            elif 'B' in reviews:
                return float(reviews.replace('B', '')) * 1000000000.0
            else:
                return float(reviews)
        except ValueError:
            return 0.0
    return 0.0

@udf(returnType=StringType())
def extract_age(content):
    soup = BeautifulSoup(content, 'html.parser')
    reviewAndAges = soup.find_all(class_= 'g1rdde')
    if reviewAndAges:
        if len(reviewAndAges) == 1:
            age = reviewAndAges[0].find('span',itemprop='contentRating').text
        elif len(reviewAndAges) == 2:
            age = reviewAndAges[1].find('span',itemprop='contentRating').text
        elif len(reviewAndAges) == 3:
            age = reviewAndAges[2].find('span',itemprop='contentRating').text
        elif len(reviewAndAges) == 4:
            age = reviewAndAges[3].find('span',itemprop='contentRating').text
        elif len(reviewAndAges) == 5:
            age = reviewAndAges[4].find('span',itemprop='contentRating').text
        return age
    else:
        return 0

@udf(returnType=DoubleType())
def extract_download(content):
    soup = BeautifulSoup(content, 'html.parser')
    d = ''
    downloads = soup.find_all(class_='ClM7O')
    if downloads:
        if len(downloads) == 1:
            return 0.0
        if len(downloads) == 2:
            d = downloads[0].text
        else:
            d = downloads[1].text
        tmp = re.sub(r'[^\d]', '', d)
        try:
            if 'K' in d:
                return float(tmp) * 1000.0
            if 'M' in d:
                return float(tmp) * 1000000.0
            if 'B' in d:
                return float(tmp) * 1000000000.0
            return float(tmp)
        except ValueError:
            return 0.0
    return 0.0

@udf(returnType=StringType())
def extract_classify(content):
    soup = BeautifulSoup(content, 'html.parser')
    classify = soup.find_all(class_='Uc6QCc')
    if not classify:
        return None
    text = classify[0].text
    start = 0

    for i in range(len(text)):
        if text[i].isupper():
            start = i
            break

    result = text[start]

    for i in range(start + 1, len(text)):
        if text[i].isalpha():
            result += text[i]
            if i + 1 < len(text) and text[i + 1].isupper():
                break
    return result

def transform(classification):
    df = spark.read.schema(schema_hdfs).parquet('hdfs://namenode:9000/' + classification + '/' + runtime)
    df_ = df.withColumn("name", extract_name(col("content"))) \
            .withColumn("company", extract_company(col("content"))) \
            .withColumn("ratings", extract_rating(col("content"))) \
            .withColumn("reviews", extract_number_of_review(col("content"))) \
            .withColumn("age", extract_age(col("content"))) \
            .withColumn("downloads", extract_download(col("content"))) \
            .withColumn("classify", extract_classify(col("content")))
    df_ = df_.drop('url','content')
    
    #rm duplicate
    df_= df_.dropDuplicates(["name","company","ratings","reviews","age","downloads","classify"])

    # write to postgresql
    try:
        df_.write.mode('overwrite')\
            .format('jdbc')\
            .option('url', 'jdbc:postgresql://data-warehouse:5432/datawarehouse')\
            .option('dbtable', classification + '_' + runtime)\
            .option('user','datawarehouse')\
            .option('password','datawarehouse')\
            .option('driver','org.postgresql.Driver')\
            .save()
        print("-------------DU LIEU DUOC GHI THANH CONG---------------------------")
    except Exception as e:
        print("--------------------GHI DU LIEU THAT BAI--------------------------------", e)
    
if __name__ == '__main__':
    runtime = datetime.now().strftime('%d%m%y')
    # runtime = '301124'
    spark = SparkSession.builder.appName('transform') \
        .config('spark.jars', '/opt/airflow/code/postgresql-42.2.5.jar').getOrCreate()
    transform('game_phone')
    transform('game_tablet')


