import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *


files = [
{"file": "map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"bulk_rides"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]
for file in files:
    file_name=file["file"]
    url =f"https://dluberdepro.blob.core.windows.net/raw/ingestion/{file_name}.json?sp=r&st=2026-04-30T20:53:03Z&se=2026-05-01T05:08:03Z&spr=https&sv=2025-11-05&sr=c&sig=AqGfDbPh7nYr1LDj%2FGJ7bFQ4T0A9qGgg%2FglcKFM7JRQ%3D"
    df=pd.read_json(url)
     
    df_spark = spark.createDataFrame(df)
 
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"uber_project.bronze.{file['file']}")




     
