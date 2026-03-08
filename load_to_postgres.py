import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

def create_spark_session():
    # Fix for WSL networking / NullPointerException
    os.environ['SPARK_LOCAL_IP'] = "127.0.0.1"

    conf = SparkConf() \
        .setAppName("Bitcoin_Silver_to_Postgres") \
        .setMaster("local[*]") \
        .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0") \
        .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
        .set("spark.hadoop.fs.s3a.access.key", "admin") \
        .set("spark.hadoop.fs.s3a.secret.key", "password123") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .set("spark.hadoop.fs.s3a.connection.establish.timeout", "10000") \
        .set("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .set("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .set("spark.hadoop.fs.s3a.listing.cache.expiration", "1000") \
        .set("spark.hadoop.fs.s3a.change.detection.mode", "none") \
        .set("spark.hadoop.fs.s3a.change.detection.policy", "none")

    return SparkSession.builder.config(conf=conf).getOrCreate()

def main():
    spark = create_spark_session()
    silver_layer_path = "s3a://processed-data/bitcoin_prices"
    
    print(f"Step 1: Attempting to read from {silver_layer_path}...")
    try:
        df = spark.read.parquet(silver_layer_path)
        print("Success! Data preview:")
        df.show(5)
        
        print("Step 2: Writing to PostgreSQL...")
        jdbc_url = "jdbc:postgresql://localhost:5432/warehouse" 

        properties = {
            "user": "admin",           # <--- Change this
            "password": "password123", # <--- Change this
            "driver": "org.postgresql.Driver"
        }
        
        df.write.jdbc(url=jdbc_url, table="bitcoin_prices", mode="append", properties=properties)
        print("Pipeline Complete.")

    except Exception as e:
        print(f"\n--- ERROR --- \n{e}")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()


