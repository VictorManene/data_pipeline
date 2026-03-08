from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def transform_data():
    # 1. Start Session with System-Level Overrides
    # We add purge.age here as a system property just to be safe
    spark = SparkSession.builder \
        .appName("BitcoinTransform") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.extraJavaOptions", "-Dfs.s3a.connection.timeout=60000 -Dfs.s3a.connection.establish.timeout=60000 -Dfs.s3a.multipart.purge.age=86400") \
        .master("local[*]") \
        .getOrCreate()

    # 2. Configure the Hadoop Live Object
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    
    # Credentials & Endpoint
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
    hadoop_conf.set("fs.s3a.access.key", "admin")
    hadoop_conf.set("fs.s3a.secret.key", "password123")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # KILLING ALL SUFFIX BUGS (60s, 24h, 30m)
    # We replace time strings with pure Long values (seconds or milliseconds)
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
    hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
    hadoop_conf.set("fs.s3a.connection.idle.time", "60000")
    hadoop_conf.set("fs.s3a.multipart.purge.age", "86400") # 24 hours in seconds
    hadoop_conf.set("fs.s3a.listing.cache.expiration", "180") # 3 minutes in seconds

    print("Step 1: Reading Raw JSON from Bronze Layer (MinIO)...")
    try:
        df = spark.read.json("s3a://raw-data/*.json")
        
        print("Step 2: Cleaning and Flattening the Data...")
        silver_df = df.select(
            col("bitcoin.usd").alias("price_usd"),
            col("bitcoin.usd_market_cap").alias("market_cap"),
            col("bitcoin.ingested_at").alias("ingested_at"),
            col("bitcoin.source").alias("source")
        )

        silver_df = silver_df.withColumn("processed_at", current_timestamp())
        silver_df.show()

        print("Step 3: Saving to Silver Layer (Parquet format)...")
        silver_df.write.mode("append").parquet("s3a://processed-data/bitcoin_prices")
        
        print("Transformation Complete!")
        
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    transform_data()