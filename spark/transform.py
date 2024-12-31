from pyspark.sql import SparkSession
from pyspark.sql.types import *
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

schema_hdfs = StructType([StructField('url', StringType(), True),StructField('content', StringType(), True),])
def extract_name(names):
    if names:
        for i in names:
            if i.find('span', class_='AfwdI', itemprop = 'name'):
                return i.text
    else:
        return None
        
def extract_company(companys):
    return companys[0].text if companys else None
   
def extract_rating(ratings):
    if ratings:
        try:
            return float(ratings[0].text)
        except ValueError:
            return 0.0
    return 0.0

def extract_number_of_review(reviewAndAges):
    if reviewAndAges:
        if len(reviewAndAges) <= 2:
            return 0.0
        reviews = reviewAndAges[0].text.strip()
        reviews = str(reviews).split(' ')[0]
        if 'K' in reviews:
            number = float(reviews.replace('K', ''))
            return number * 1000.0
        if 'M' in reviews:
            number = float(reviews.replace('M', ''))
            return number * 1000000.0
        if 'B' in reviews:
            number = float(reviews.replace('B', ''))
            return number * 1000000000.0
        return float(reviews)
    else:
        return 0.0

def extract_age(reviewAndAges):
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

def extract_download(downloads):
    d = ''
    if downloads:
        if len(downloads) == 1:
            return 0.0
        if len(downloads) == 2:
            d = downloads[0].text
        else:
            d = downloads[1].text
        tmp = re.sub(r'[^\d]', '', d)
        if 'K' in d:
            return float(tmp) * 1000.0
        if 'M' in d:
            return float(tmp) * 1000000.0
        if 'B' in d:
            return float(tmp) * 1000000000.0
        return float(tmp)
    else:
        return 0.0
    
def extract_classify(classify):
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

    listUrl = df.select('url').collect()
    listContent = df.select('content').collect()

    data = []
    for i in range(len(listUrl)):
    # def parse_content(row):
        soup = BeautifulSoup(listContent[i].content,'html.parser')
        
        name = soup.find_all('h1')
        company = soup.find_all(class_ = 'Vbfug auoIOc')
        rating = soup.find_all(class_ = 'jILTFe')
        reviewAndAge = soup.find_all(class_= 'g1rdde')
        downloads = soup.find_all(class_='ClM7O')
        classify = soup.find_all(class_='Uc6QCc')
        describe = soup.find_all(class_='bARER')
        lastVersion = soup.find_all(class_ = 'xg1aie')

        field ={}

        if len(name) == 0: return None
            # continue
        field['name'] = extract_name(name)
        field['company'] = extract_company(company)
        field['ratings'] = extract_rating(rating)
        field['reviews'] = extract_number_of_review(reviewAndAge)
        field['age'] = extract_age(reviewAndAge)
        field['downloads'] = extract_download(downloads)
        field['classify'] = extract_classify(classify)
        field['describe'] = describe[0].text if describe else None
        field['lastVersion'] = lastVersion[0].text if lastVersion else None

        data.append(field)
    
    # write to postgresql
    # parsed_data = df.rdd.map(parse_content).filter(lambda x: x is not None)
    df_ = spark.createDataFrame(data = data, schema = schema)

    try:
        df_.write.mode('overwrite')\
            .format('jdbc')\
            .option('url', 'jdbc:postgresql://data-warehouse:5432/datawarehouse')\
            .option('dbtable', classification + '_' + runtime)\
            .option('user','datawarehouse')\
            .option('password','datawarehouse')\
            .option('driver','org.postgresql.Driver')\
            .save()
        print("Write to PostgreSql successfully")
    except Exception as e:
        print("can not write to Postgresql", e)
    
if __name__ == '__main__':
    runtime = datetime.now().strftime('%d%m%y')
    # runtime = '301124'
    spark = SparkSession.builder.appName('transform') \
        .config('spark.jars', '/opt/airflow/code/postgresql-42.2.5.jar').getOrCreate()
    transform('game_phone')
    transform('game_tablet')


