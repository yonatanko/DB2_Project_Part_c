import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

def init_spark(app_name: str):

  spark = SparkSession.builder.appName(app_name).getOrCreate()
  sc = spark.sparkContext
  return spark, sc

def main():
  spark, sc = init_spark('demo')
  print(spark.version)
  print('CPUs:', os.cpu_count())

if __name__ == "__main__":
  main()