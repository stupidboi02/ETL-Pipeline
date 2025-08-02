# from pyspark.sql import SparkSession
# from datetime import datetime


# jdbc_url = "jdbc:postgresql://data-warehouse:5432/datawarehouse"
# properties = {
#     "user": "datawarehouse",
#     "password": "datawarehouse",
#     "driver": "org.postgresql.Driver"
# }
# def load(classification):
#     existing_data = spark.read.jdbc(url=jdbc_url, table = classification + "_" + runtime, properties=properties)
#     existing_data.write.jdbc(url=jdbc_url, table="google_store_game", mode="append", properties=properties)

# if __name__ == '__main__':
#     runtime = datetime.now().strftime('%d%m%y')
#     spark = SparkSession.builder.appName('combine and load') \
#         .config('spark.jars', '/opt/airflow/code/postgresql-42.2.5.jar').getOrCreate()
#     load('game_phone')
#     load('game_tablet')