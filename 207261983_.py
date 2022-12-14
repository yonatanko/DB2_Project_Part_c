from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
import os
import time
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pyspark.sql.functions as f
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.sql.types import *

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
    .config("spark.driver.memory", "9g") \
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
    .option("maxOffsetsPerTrigger", 999999) \
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
    data = data.filter(f.col("gt") != "null")
    data = data.withColumn("Arrival_Time", f.from_unixtime(f.col("Arrival_Time") / 1000, 'MM-dd-yyyy HH:mm:ss'))
    data = data.withColumn("Creation_Time", f.from_unixtime(f.col("Creation_Time") / 1000000000, 'MM-dd-yyyy HH:mm:ss'))
    data = data.withColumn("Arrival_Time", f.to_timestamp(data.Arrival_Time, 'MM-dd-yyyy HH:mm:ss'))
    data = data.withColumn("Creation_Time", f.to_timestamp(data.Creation_Time, 'MM-dd-yyyy HH:mm:ss'))

    data = data.withColumn("Hour", f.hour(f.col("Creation_Time")))\
        .withColumn("Minute", f.minute(f.col("Creation_Time"))) \
        .withColumn("Date", f.date_format(f.col("Creation_Time"), "yyyy-MM-dd"))\
        .withColumn("Day", f.date_format('Date', 'EEEE'))

    # giving indexes to data labels and categorical features
    labelIndexer = StringIndexer(inputCol="gt", outputCol="indexedLabel").fit(data)
    output = labelIndexer.transform(data)
    transformer_pipeline = Pipeline(stages=[
        StringIndexer(inputCol="Device", outputCol="Device_index"),
        StringIndexer(inputCol="User", outputCol="User_index"),
        StringIndexer(inputCol="Day", outputCol="Day_index"),
        StringIndexer(inputCol="Date", outputCol="Date_index"),
        OneHotEncoderEstimator(inputCols=["Device_index", "User_index", "Day_index", "Date_index"], outputCols=["Device_vec", "User_vec", "Day_vec", "Date_vec"]),
        VectorAssembler(
            inputCols=["Hour", "Minute", "x", "y", "z", "Device_vec", "User_vec", "Day_vec", "Date_vec"],
            outputCol="indexedFeatures")
    ])
    output = transformer_pipeline.fit(output).transform(output)
    return output, labelIndexer


trainingData, _ = transformations(static_df)
print("initial train size: " + str(trainingData.count()) + " rows")

total_sum = 0
total_rows = 0
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10, maxDepth=15)


def random_forest(data, epoch_num):
    global total_rows, total_sum, trainingData, rf
    print()
    print("epoch:" + str(epoch_num))

    time.sleep(5)
    output, labelIndexer = transformations(data)
    testData = output
    num_rows = testData.count()
    print("batch size: " + str(num_rows))
    # Train a RandomForest model.
    model = rf.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)
    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print("Test Accuracy " + str(accuracy*100) + "%")
    trainingData = trainingData.union(testData)
    print("current train size: " + str(trainingData.count()) + " rows")

    total_sum += accuracy

    if epoch_num == 6:
        print()
        print("finished reading all the data")
        print("avg accuracy: " + str(total_sum/(epoch_num+1)))
        print()


def main():
    global total_sum, total_rows

    stream_df \
        .writeStream.foreachBatch(random_forest) \
        .start() \
        .awaitTermination()


if __name__ == "__main__":
    main()
