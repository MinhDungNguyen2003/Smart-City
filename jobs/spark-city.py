from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json
from pyspark.sql import DataFrame

def main():
    spark = SparkSession.builder.appName("SparkCityStreaming")\
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .config("spark.sql.adaptive.enabled", "false").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),  
        StructField("year", IntegerType(), True),  
        StructField("fuel", StringType(), True),  
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])

    cameraSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("speed", DoubleType(), True),
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("windDirection", StringType(), True),
        StructField("airQualityIndex", DoubleType(), True),
        StructField("uvIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("emergencyType", StringType(), True),
        StructField("description", StringType(), True),
        StructField("status", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", 'broker:29092')
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json("value", schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
            )

    def write_to_s3(input: DataFrame, checkpointFolder, outputFolder):
        return (input.writeStream
                .format("parquet")
                .option("checkpointLocation", checkpointFolder)
                .option("path", outputFolder)
                .outputMode("append")
                .start()
            )

    vehicleDF = read_kafka_topic("vehicle_data", vehicleSchema)
    gpsDF = read_kafka_topic("gps_data", gpsSchema)
    cameraDF = read_kafka_topic("camera_data", cameraSchema)
    weatherDF = read_kafka_topic("weather_data", weatherSchema)
    emergencyDF = read_kafka_topic("emergency_data", emergencySchema)

    query1 = write_to_s3(vehicleDF, "s3a://spark-streaming-data-smart-city/checkpoints/vehicle_data",
                         "s3a://spark-streaming-data-smart-city/data/vehicle_data")

    query2 = write_to_s3(gpsDF, "s3a://spark-streaming-data-smart-city/checkpoints/gps_data",
                         "s3a://spark-streaming-data-smart-city/data/gps_data")

    query3 = write_to_s3(cameraDF, "s3a://spark-streaming-data-smart-city/checkpoints/camera_data",
                         "s3a://spark-streaming-data-smart-city/data/camera_data")

    query4 = write_to_s3(weatherDF, "s3a://spark-streaming-data-smart-city/checkpoints/weather_data",
                         "s3a://spark-streaming-data-smart-city/data/weather_data")

    query5 = write_to_s3(emergencyDF, "s3a://spark-streaming-data-smart-city/checkpoints/emergency_data",
                         "s3a://spark-streaming-data-smart-city/data/emergency_data")

    query5.awaitTermination()


if __name__ == "__main__":
    main()
