from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import os
import time
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pyspark.sql.functions as f
from pyspark.ml.feature import OneHotEncoderEstimator

SCHEMA = StructType([StructField("Arrival_Time", LongType(), True),
                     StructField("Creation_Time", LongType(), True),
                     StructField("Device", StringType(), True),
                     StructField("Index", LongType(), True),
                     StructField("Model", StringType(), True),
                     StructField("User", StringType(), True),
                     StructField("gt", StringType(), True),
                     StructField("x", DoubleType(), True),
                     StructField("y", DoubleType(), True),
                     StructField("z", DoubleType(), True)])

spark = SparkSession.builder.appName('demo_app') \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .getOrCreate()

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8,com.microsoft.azure:spark-mssql-connector:1.0.1"
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
topic = "activities"

stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", False) \
    .option("maxOffsetsPerTrigger", 10000) \
    .load() \
    .select(f.from_json(f.decode("value", "US-ASCII"), schema=SCHEMA).alias("value")).select("value.*")

query = stream_df \
    .writeStream.queryName("query") \
    .format("memory") \
    .outputMode("append") \
    .start()

static_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", False) \
    .option("maxOffsetsPerTrigger", 10000) \
    .load() \
    .select(f.from_json(f.decode("value", "US-ASCII"), schema=SCHEMA).alias("value")).select("value.*")


def transformations(data):
    # giving indexes to data labels
    labelIndexer = StringIndexer(inputCol="gt", outputCol="indexedLabel").fit(data)
    output = labelIndexer.transform(data)
    output.show(5, truncate=False)
    # giving indexes to categorial features
    output = StringIndexer(inputCol="Device", outputCol="Device_index").fit(output).transform(output)
    output.show(5, truncate=False)
    output = StringIndexer(inputCol="User", outputCol="User_index").fit(output).transform(output)
    output.show(5, truncate=False)
    # # Creating sparse vectors out of indexes
    # output = OneHotEncoderEstimator(inputCol="Device_index", outputCol="Device_vec").fit(output).transform(output)
    # output.show(5, truncate=False)
    # output = OneHotEncoderEstimator(inputCol="User_index", outputCol="User_vec").fit(output).transform(output)
    # output.show(5, truncate=False)
#     # Assemblig one feature vector
#     assembler = VectorAssembler(
#         inputCols=["Device_vec", "User_vec", "Creation_Time", "Arrival_Time", "x", "y", "z"],
#         outputCol="features")
#     output = assembler.transform(output)
#     output.show(5, truncate=False)
#     # giving indexes to features column
#     featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures").fit(output)
#     output = featureIndexer.transform(output)
#
#     return output, labelIndexer
#
#
# def random_forest(data):
#     output, labelIndexer = transformations(data)
#     # Split the data into training and test sets (30% held out for testing)
#     (trainingData, testData) = output.randomSplit([0.7, 0.3])
#
#     # Train a RandomForest model.
#     rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=22, maxDepth=19)
#     model = rf.fit(trainingData)
#
#     # Make predictions.
#     predictions = model.transform(testData)
#     labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)
#     output = labelConverter.transform(predictions)
#     # Select (prediction, true label) and compute test error
#     evaluator = MulticlassClassificationEvaluator(
#         labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
#     accuracy = evaluator.evaluate(predictions)
#     print("Test Accuracy " + str(accuracy))


def main():
    transformations(static_df)
    # random_forest(static_df)


if __name__ == "__main__":
    main()
