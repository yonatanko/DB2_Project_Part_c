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

stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "activities") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", False) \
    .option("maxOffsetsPerTrigger", 100000) \
    .load() \
    .select(f.from_json(f.decode("value", "US-ASCII"), schema=SCHEMA).alias("value")).select("value.*")

static_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", "static") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", False) \
    .option("maxOffsetsPerTrigger", 10000) \
    .load() \
    .select(f.from_json(f.decode("value", "US-ASCII"), schema=SCHEMA).alias("value")).select("value.*")


def transformations(data):
    # giving indexes to data labels and categorical features
    labelIndexer = StringIndexer(inputCol="gt", outputCol="indexedLabel").fit(data)
    output = labelIndexer.transform(data)
    # giving indexes to categorical features
    output = StringIndexer(inputCol="Device", outputCol="Device_index").fit(output).transform(output)
    output = StringIndexer(inputCol="User", outputCol="User_index").fit(output).transform(output)
    # Creating sparse vectors out of indexes
    encoder = OneHotEncoderEstimator(inputCols=["Device_index", "User_index"], outputCols=["Device_vec", "User_vec"]).fit(output)
    output = encoder.transform(output)
    # Assembling one feature vector
    assembler = VectorAssembler(
        inputCols=["Creation_Time", "Arrival_Time", "x", "y", "z", "Device_vec", "User_vec"],
        outputCol="indexedFeatures")
    output = assembler.transform(output)

    return output, labelIndexer


trainingData, _ = transformations(static_df)
print("finished transformations on static data :)")
print("train size: " + str(trainingData.count()) + " rows")


def random_forest(data, epoch_num):
    global trainingData
    time.sleep(5)
    output, labelIndexer = transformations(data)
    testData = output
    print("finished transformations on test data :)")
    # Train a RandomForest model.
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=22, maxDepth=19)
    model = rf.fit(trainingData)
    print("finished fitting :)")

    # Make predictions.
    predictions = model.transform(testData)
    print("finished predicting :)")
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)
    output = labelConverter.transform(predictions)
    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print("epoch:"+str(epoch_num))
    print("Test Accuracy " + str(accuracy))
    trainingData = trainingData.union(testData)
    print("finished union :)")
    print("train size: " + str(trainingData.count()) + " rows")


def main():
    stream_df \
        .writeStream.foreachBatch(random_forest) \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()