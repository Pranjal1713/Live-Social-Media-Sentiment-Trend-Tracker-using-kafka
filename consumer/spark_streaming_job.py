# social-media-sentiment-tracker/consumer/spark_streaming_job_fixed.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lower, udf, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging
import os
import shutil

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    """Calculate sentiment score using VADER"""
    if text is None or text.strip() == "":
        return 0.0
    try:
        return analyzer.polarity_scores(text)['compound']
    except Exception as e:
        logger.error(f"Error calculating sentiment: {e}")
        return 0.0

def get_sentiment_label(score):
    """Convert sentiment score to label"""
    if score is None:
        return "neutral"
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

# Register UDFs
sentiment_udf = udf(get_sentiment_score, FloatType())
sentiment_label_udf = udf(get_sentiment_label, StringType())

def clean_checkpoint_directories():
    """Clean up checkpoint directories to avoid conflicts"""
    checkpoint_dirs = [
        "/opt/spark-data/checkpoint",
        "/opt/spark-data/checkpoint/aggregated"
    ]
    
    for checkpoint_dir in checkpoint_dirs:
        if os.path.exists(checkpoint_dir):
            try:
                shutil.rmtree(checkpoint_dir)
                logger.info(f"Cleaned checkpoint directory: {checkpoint_dir}")
            except Exception as e:
                logger.error(f"Error cleaning checkpoint directory {checkpoint_dir}: {e}")

def ensure_output_directories():
    """Ensure output directories exist"""
    output_dirs = [
        "/opt/spark-data/output",
        "/opt/spark-data/output/aggregated",
        "/opt/spark-data/checkpoint",
        "/opt/spark-data/checkpoint/aggregated"
    ]
    
    for output_dir in output_dirs:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Ensured directory exists: {output_dir}")

def main():
    # Clean up old checkpoints to avoid conflicts
    clean_checkpoint_directories()
    ensure_output_directories()
    
    # Create Spark Session with proper configurations
    spark = SparkSession \
        .builder \
        .appName("SocialMediaSentimentTracker") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.master", "local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.maxResultSize", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation.autoDelete", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")

    # Define schema for the incoming JSON data
    location_schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ])
    
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("user", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("user_followers", IntegerType(), True),
        StructField("likes", IntegerType(), True),
        StructField("retweets", IntegerType(), True),
        StructField("location", location_schema, True),
        StructField("timestamp", StringType(), True)
    ])

    # Read from Kafka
    logger.info("Setting up Kafka stream...")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "social-media-posts") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Parse JSON data from Kafka
    posts_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_timestamp", current_timestamp())

    # Process the data
    processed_df = posts_df \
        .filter(col("text").isNotNull() & (col("text") != "")) \
        .withColumn("city", col("location.city")) \
        .withColumn("country", col("location.country")) \
        .drop("location") \
        .withColumn("cleaned_text", lower(col("text"))) \
        .withColumn("sentiment_score", sentiment_udf(col("cleaned_text"))) \
        .withColumn("sentiment_label", sentiment_label_udf(col("sentiment_score"))) \
        .withColumn("hashtags", expr(r"regexp_extract_all(cleaned_text, '#(\w+)', 1)"))

    # Console output for debugging
    console_query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 5) \
        .trigger(processingTime='15 seconds') \
        .queryName("console_debug") \
        .start()

    logger.info("Console debug stream started")

    # File output stream - SINGLE STREAM ONLY
    file_query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/opt/spark-data/output") \
        .option("checkpointLocation", "/opt/spark-data/checkpoint") \
        .option("maxFilesPerTrigger", "1") \
        .trigger(processingTime='30 seconds') \
        .queryName("file_output") \
        .start()

    logger.info("File output stream started - writing to /opt/spark-data/output")

    # Monitor stream health
    def monitor_streams():
        active_streams = spark.streams.active
        logger.info(f"Active streams: {len(active_streams)}")
        for stream in active_streams:
            logger.info(f"Stream: {stream.name}, Status: {stream.isActive}")
            if stream.lastProgress:
                batch_id = stream.lastProgress.get("batchId", "N/A")
                input_rows = stream.lastProgress.get("inputRowsPerSecond", 0)
                logger.info(f"Batch ID: {batch_id}, Input rows/sec: {input_rows}")

    # Wait for termination
    try:
        logger.info("Waiting for streams to process data...")
        
        # Monitor streams periodically
        import time
        while True:
            time.sleep(30)
            monitor_streams()
            
            # Check if any streams have failed
            active_streams = spark.streams.active
            if not active_streams:
                logger.warning("No active streams found!")
                break
                
            failed_streams = [s for s in active_streams if not s.isActive]
            if failed_streams:
                logger.error(f"Failed streams detected: {[s.name for s in failed_streams]}")
                break
                
    except KeyboardInterrupt:
        logger.info("Stopping all streams...")
        for stream in spark.streams.active:
            try:
                stream.stop()
                logger.info(f"Stopped stream: {stream.name}")
            except Exception as e:
                logger.error(f"Error stopping stream: {e}")
    except Exception as e:
        logger.error(f"Stream processing error: {e}")
    finally:
        # Cleanup
        logger.info("Shutting down Spark session...")
        spark.stop()

if __name__ == "__main__":
    main()