import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["BUCKET", "LOAD_DATE"])
spark = GlueContext(SparkContext.getOrCreate()).spark_session

raw_path = f"s3://{args['BUCKET']}/raw/employees/load_date={args['LOAD_DATE']}/"
out_path = f"s3://{args['BUCKET']}/curated/employees/"

df = spark.read.json(raw_path)

clean = (
    df.withColumn("id", F.col("id").cast("int"))
      .withColumn("salary", F.col("salary").cast("double"))
      .dropna(subset=["id"])
      .dropDuplicates(["id"])
      .withColumn(
          "salary_band",
          F.when(F.col("salary") < 50000, "Low")
           .when(F.col("salary") < 100000, "Medium")
           .otherwise("High"),
      )

)

day_path = f"{out_path}load_date={args['LOAD_DATE']}/"
clean.write.mode("overwrite").parquet(day_path)

print(f"Wrote {clean.count()} rows to {day_path} in parquet format.")