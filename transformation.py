from pyspark.sql.functions import to_date, col, asc, round
from utils.logger import get_logger
logger = get_logger(__name__)

## THIS CODE PROCESS ALL JSON FILES IN PROVIDED FOLDER AND WRITE TO DELTA TABLE

def transform_bitcoin_to_delta(input_path, output_path):
    logger.info(f"Transforming data from {input_path} to {output_path}")

    try:
        df = spark.read.format("json") \
            .option("multiLine", True) \
            .load(input_path)

        df_transformed = df.withColumn(
            "created_at",
            to_date(col("created_at"), "dd/MM/yyyy")
        ).withColumn(
            "brl_price",
            round(col("brl_price").cast("double"),2)
        ).withColumn(
            "dolar_price",
            round(col("dolar_price").cast("double"),2)
        )

        df_transformed = df_transformed.select("created_at", "brl_price", "dolar_price") \
            .dropDuplicates(["created_at"])\
            .orderBy(asc("created_at"))

        df_transformed.write.format("delta") \
            .mode("overwrite") \
            .save(output_path)
        
        logger.info("Processing completed!")
    except Exception as e:
        logger.error(f"Error processing data: {e}")

if __name__ == "__main__":
    transform_bitcoin_to_delta("/Volumes/workspace/landing/bitcoin", "/Volumes/workspace/processed/bitcoin")