from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Load")\
    .config("spark.jars", "/opt/airflow/code/postgresql-42.2.5.jar")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://app-warehouse:5432/datawarehouse"
properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

staging_df = spark.read.jdbc(url= jdbc_url, table="app_store", properties=properties)

staging_df.show()

# Chuan hoa
staging_df = staging_df.dropDuplicates()\
    .withColumn("name", col('name').cast("String"))\
    .withColumn("company", col('company').cast("string"))\
    .withColumn("classify", col("classify").cast("string"))\
    .withColumn("device", col("device").cast("string"))\
    .withColumn("category", col("category").cast("string"))\
    .withColumn("ratings", col("ratings").cast("double"))\
    .withColumn("reviews", col("reviews").cast("bigint"))\
    .withColumn("downloads", col('downloads').cast("bigint"))\
    .withColumn("scraped_date", col("scraped_date").cast("date"))

staging_df.cache()

# Load dim_date
dim_date_df = staging_df.select("scraped_date").distinct()\
                .withColumn("year", year(col('scraped_date')))\
                .withColumn("month", month(col('scraped_date')))\
                .withColumn("day", dayofmonth(col('scraped_date')))\
                .withColumn("quarter", quarter(col('scraped_date')))\
                
try:
    existing_dim_date_df = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=properties)
    dim_date_to_insert = dim_date_df.join(existing_dim_date_df, on="scraped_date", how="left_anti")
except Exception as e:
    print(f"{e}: Sẽ tạo mới toàn bộ.")
    dim_date_to_insert = dim_date_df

if dim_date_to_insert.count() > 0:
    print(f"thêm {dim_date_to_insert.count()} ngày mới vào dim_date")
    dim_date_to_insert.write \
        .format('jdbc') \
        .option("url", jdbc_url) \
        .options(**properties) \
        .option('dbtable', 'dim_date') \
        .mode("append") \
        .save()
else:
    print("Không có ngày mới")

# Load dim_company
dim_company_df = staging_df.select("company").distinct()

try:
    existing_dim_company_df = spark.read.jdbc(url=jdbc_url, table="dim_company", properties=properties)
    dim_company_to_insert = dim_company_df.join(existing_dim_company_df, on="company", how="left_anti")
except Exception as e:
    print(f"Bảng dim_company chưa tồn tại {e}: Tạo mới toàn bộ.")
    dim_company_to_insert = dim_company_df

if dim_company_to_insert.count() > 0:
    print(f"thêm {dim_company_to_insert.count()} công ty mới vào dim_company")
    dim_company_to_insert.write \
        .format('jdbc') \
        .option("url", jdbc_url) \
        .options(**properties) \
        .option('dbtable', 'dim_company') \
        .mode("append") \
        .save()
else:
    print("Không có công ty mới.")

# Load dim_classify
dim_classify_df = staging_df.select("classify").distinct()\
                    .withColumnRenamed("classify", "classify_name")

try:
    existing_dim_classify_df = spark.read.jdbc(url=jdbc_url, table='dim_classify', properties=properties)
    dim_classify_to_insert = dim_classify_df.join(existing_dim_classify_df, on = "classify_name", how="left_anti")
except Exception as e:
    print(e," Bảng dim_classify chưa tồn tại")
    dim_classify_to_insert = dim_classify_df

if dim_classify_to_insert.count() > 0:
    dim_classify_to_insert.write\
        .format('jdbc')\
        .option(("url"), jdbc_url) \
        .options(**properties) \
        .option('dbtable','dim_classify')\
        .mode("append")\
        .save()
else:
    print("Không có thể loại mới")

# Load dim_category
dim_category_df = staging_df.select("category").distinct()\
                .withColumnRenamed("category", "category_type")
try:
    existing_dim_category_df = spark.read.jdbc(url=jdbc_url, table = "dim_category", properties=properties)
    dim_category_to_insert = dim_category_df.join(existing_dim_category_df, on="category_type", how='left_anti')
except Exception as e:
    print(e,"Chưa có bảng dim_category")
    dim_category_to_insert = dim_category_df

if dim_category_to_insert.count() > 0:
    dim_category_to_insert.write\
        .format('jdbc')\
        .option(("url"), jdbc_url) \
        .options(**properties) \
        .option('dbtable','dim_category')\
        .mode("append")\
        .save()
else:
    print("Không có thể loại mới")
# Load dim_app
# Đọc lại dim_company từ DB để lấy company_id
company_df = spark.read.jdbc(url=jdbc_url, table="dim_company", properties=properties)
dim_app_df = staging_df.select("name","company").distinct()\
    .join(company_df, on ='company', how='inner')\
    .select(staging_df.name, company_df.company_id)
try:
    existing_dim_app_df = spark.read.jdbc(url = jdbc_url, properties=properties, table = 'dim_app')
    dim_app_to_insert = dim_app_df.join(existing_dim_app_df, 
        (dim_app_df.name == existing_dim_app_df.name) & (dim_app_df.company_id == existing_dim_app_df.company_id), 
        how='left_anti')
except Exception as e:
    print(e, "Không có bảng dim_app")
    dim_app_to_insert = dim_app_df
if dim_app_to_insert.count() > 0:
    dim_app_to_insert.write\
        .format('jdbc')\
        .option(("url"), jdbc_url) \
        .options(**properties) \
        .option('dbtable','dim_app')\
        .mode("append")\
        .save()
else:
    print("Không có app mới")
# Load dim_device
dim_device_df = staging_df.select("device").distinct().withColumnRenamed("device", "device_type")
try:
    existing_dim_device_df = spark.read.jdbc(url=jdbc_url, properties=properties, table = 'dim_device')
    dim_device_to_insert = dim_device_df.join(existing_dim_device_df, on='device_type', how='left_anti')
except Exception as e:
    print(e, "Không có bản dim_device")
    dim_device_to_insert = dim_device_df
if dim_device_to_insert.count() > 0:
    dim_device_to_insert.write\
        .format('jdbc')\
        .option(("url"), jdbc_url) \
        .options(**properties) \
        .option('dbtable','dim_device')\
        .mode("append")\
        .save()
else:
    print("Không có thiết bị mới")
# Load Fact_table
# Đọc lại từ bảng để lấy id
date_df = spark.read.jdbc(url=jdbc_url, table="dim_date", properties=properties)
classify_df = spark.read.jdbc(url=jdbc_url, table='dim_classify', properties=properties)
category_df = spark.read.jdbc(url=jdbc_url, table = "dim_category", properties=properties)
app_df = spark.read.jdbc(url = jdbc_url, properties=properties, table = 'dim_app')
device_df = spark.read.jdbc(url=jdbc_url, properties=properties, table = 'dim_device')

fact_app_metrics_df = staging_df\
    .join(app_df, staging_df.name == app_df.name, how='inner')\
    .join(date_df, staging_df.scraped_date == date_df.scraped_date, how='inner')\
    .join(classify_df, staging_df.classify == classify_df.classify_name, how='inner')\
    .join(category_df, staging_df.category == category_df.category_type, how='inner')\
    .join(device_df, staging_df.device == device_df.device_type, how='inner')\
    .select(
        app_df.app_id,
        app_df.company_id,
        date_df.date_id,
        classify_df.classify_id,
        category_df.category_id,
        device_df.device_id,
        staging_df.ratings,
        staging_df.reviews,
        staging_df.downloads,
        staging_df.age
    )

# Ghi dữ liệu vào bảng fact_app_metrics
if fact_app_metrics_df.count() > 0:
    fact_app_metrics_df.write\
        .format('jdbc')\
        .option("url", jdbc_url)\
        .options(**properties)\
        .option('dbtable', 'fact_app_metrics')\
        .mode("overwrite")\
        .save()
else:
    print("Không có dữ liệu mới cho fact_app_metrics.")


