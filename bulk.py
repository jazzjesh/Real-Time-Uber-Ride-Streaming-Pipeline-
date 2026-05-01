from pyspark import pipelines as dp
from pyspark.sql.functions import *

# =============================================
# BULK RIDES - Disabled due to missing Azure storage credentials
# =============================================
# To enable this dataset, add the Azure storage account key to pipeline configuration:
# fs.azure.account.key.dluberdepro.dfs.core.windows.net = <your-key>
#
# INGESTION_PATH = "abfss://raw@dluberdepro.dfs.core.windows.net/ingestion"
# 
# @dp.table(
#     name="bulk_rides",
#     comment="Initial historical bulk ride data loaded from ADLS ingestion folder"
# )
# def bulk_rides():
#     return (
#         spark.read
#         .option("multiLine", "true")
#         .json(f"{INGESTION_PATH}/bulk_rides.json")
#     )
