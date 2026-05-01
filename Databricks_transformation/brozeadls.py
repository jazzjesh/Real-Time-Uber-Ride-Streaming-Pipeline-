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
    url =f"https://dluberdepro.blob.core.windows.net/raw/ingestion/{file_name}.json?"
    df=pd.read_json(url)
     
    df_spark = spark.createDataFrame(df)
 
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"uber_project.bronze.{file['file']}")




     
