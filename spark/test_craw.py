from pyspark.sql import SparkSession
from pyspark.sql.types import *

schema = StructType([
    StructField("name", StringType(), True),
    StructField("company", StringType(), True),
    StructField("ratings", DoubleType(), True),
    StructField("reviews", IntegerType(), True),
    StructField("age", StringType(), True),
    StructField("downloads", IntegerType(), True),
    StructField("classify", StringType(), True),
    StructField("describe", StringType(), True),
    StructField("lastVersion", StringType(), True),
])

spark = SparkSession.builder.appName('transform')\
.config('spark.jars','/opt/airflow/code/postgresql-42.2.5.jar')\
.getOrCreate()

simple_data = [{'name': 'test', 'company': 'example', 'ratings': 4.5, 'reviews': 1000000, 
                'age': '4+', 'downloads': 50000, 'classify': 'Game', 
                'describe': 'Test App', 'lastVersion': '1.0'}]
test_df = spark.createDataFrame(simple_data, schema=schema)

test_df.write.mode('overwrite')\
    .format('jdbc')\
    .option('url', 'jdbc:postgresql://data-warehouse:5432/datawarehouse')\
    .option('dbtable', 'test_table')\
    .option('user', 'datawarehouse')\
    .option('password', 'datawarehouse')\
    .save()
