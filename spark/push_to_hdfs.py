# from pyspark.sql.session import SparkSession
# import os
# from datetime import datetime

# # run_time = datetime.now().strftime('%d%m%y')
# run_time = '131224'

# def push(path, classify):
#     try:
#         if os.path.exists(path):
#             df = spark.read.parquet(path)
#             df.write.mode("overwrite").parquet("hdfs://namenode:9000/" + classify + "/" + run_time)
#         else:
#             print(f"Source file not found: {path}")
#     except Exception as e:
#         print(f"Error processing game_phone: {e}")

# if __name__ == "__main__":
#     spark = SparkSession.builder \
#         .appName("extract_load") \
#         .getOrCreate()
#     # Game phone
#     phone_path = "/opt/airflow/code/data/game_phone/" + run_time + ".parquet"
#     push(phone_path, 'game_phone')
#     # Game tablet
#     tablet_path = "/opt/airflow/code/data/game_tablet/" + run_time + ".parquet"
#     push(phone_path, 'game_tablet')
    