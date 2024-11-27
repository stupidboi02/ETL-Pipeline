# from pyspark.sql.session import SparkSession
# import os

# def push(classify, runtime):
#     try:
#         path = '/opt/airflow/code/data/' + classify +'/' + runtime +'.parquet'
#         if os.path.exists(path):
#             df = spark.read.parquet(path)
#             df.write.mode("overwrite").parquet("hdfs://namenode:9000/" + classify + "/" + runtime)
#         else:
#             print(f"Source file not found: {path}")
#     except Exception as e:
#         print(f"Error processing game_phone: {e}")

# if __name__ == "__main__":
#     spark = SparkSession.builder \
#         .appName("push_manual") \
#         .getOrCreate()
    
# for i in range(3,8):
#     runtime = '2'+str(i)+'1124'
#     push('game_phone', runtime)
#     push('game_tablet',runtime)