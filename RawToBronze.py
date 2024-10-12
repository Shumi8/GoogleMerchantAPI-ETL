# Databricks notebook source
def perform_delete_insert(INGESTION_TABLE, BRONZE_TABLE_PATH, BRONZE_TABLE, deletion_column, deletion_days):
  """If delta does not exist then ingest the data fully, otherwise perform delete insert based on the time series column"""
  if not DeltaTable.isDeltaTable(spark, BRONZE_TABLE_PATH):
    raw_df = spark.read.format("bigquery").option("table", INGESTION_TABLE).load()
    raw_df.write \
          .format('delta') \
          .mode('overwrite') \
          .option("mergeSchema", "True") \
          .option('path', BRONZE_TABLE_PATH) \
          .saveAsTable(BRONZE_TABLE)
  else:
    delta_table = DeltaTable.forPath(spark, BRONZE_TABLE_PATH)
    delta_table.delete((F.col(deletion_column) > F.date_sub(F.current_date(), deletion_days)))

    raw_df = spark.read.format("bigquery") \
                       .option("table", INGESTION_TABLE) \
                       .load() \
                       .where(F.col(deletion_column) >= F.date_sub(F.current_date(), deletion_days))
    raw_df.write \
          .format('delta') \
          .mode('append') \
          .option("mergeSchema", "True") \
          .option('path', BRONZE_TABLE_PATH) \
          .saveAsTable(BRONZE_TABLE)

# COMMAND ----------

print("Delete/inserting top brands table")
perform_delete_insert(INGESTION_TABLE = top_brands_table, BRONZE_TABLE_PATH = GOOGLE_TOP_BRANDS_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_TOP_BRANDS_BRONZE_TABLE, deletion_column = 'rank_timestamp', deletion_days = 30)

print("Delete/inserting top products table")
perform_delete_insert(INGESTION_TABLE = top_products_table, BRONZE_TABLE_PATH = GOOGLE_TOP_PRODUCTS_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_TOP_PRODUCTS_BRONZE_TABLE, deletion_column = 'rank_timestamp', deletion_days = 30)

print("Delete/inserting top products inventory table")
perform_delete_insert(INGESTION_TABLE = top_products_inventory_table, BRONZE_TABLE_PATH = GOOGLE_TOP_PRODUCTS_INVENTORY_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_TOP_PRODUCTS_INVENTORY_BRONZE_TABLE, deletion_column = '_PARTITIONTIME', deletion_days = 30)

print("Delete/inserting price benchmark table")
perform_delete_insert(INGESTION_TABLE = products_price_benchmarks_table, BRONZE_TABLE_PATH = GOOGLE_PRODUCTS_PRICE_BENCHMARKS_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_PRODUCTS_PRICE_BENCHMARKS_BRONZE_TABLE, deletion_column = '_PARTITIONTIME', deletion_days = 30)

print("Delete/inserting product dimension table")
perform_delete_insert(INGESTION_TABLE = products_table, BRONZE_TABLE_PATH = GOOGLE_PRODUCTS_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_PRODUCTS_BRONZE_TABLE, deletion_column = '_PARTITIONTIME', deletion_days = 30)

print("Delete/inserting page feeds table")
perform_delete_insert(INGESTION_TABLE = page_feeds_table, BRONZE_TABLE_PATH = GOOGLE_PAGE_FEEDS_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_PAGE_FEEDS_BRONZE_TABLE, deletion_column = 'date', deletion_days = 3)

print("Delete/inserting page metrics table")
perform_delete_insert(INGESTION_TABLE = page_metrics_table, BRONZE_TABLE_PATH = GOOGLE_PAGE_METRICS_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_PAGE_METRICS_BRONZE_TABLE, deletion_column = 'date', deletion_days = 3)

print("Delete/inserting page referral table")
perform_delete_insert(INGESTION_TABLE = page_referrer_table, BRONZE_TABLE_PATH = GOOGLE_PAGE_REFERRER_BRONZE_TABLE_PATH, BRONZE_TABLE = GOOGLE_PAGE_REFERRER_BRONZE_TABLE, deletion_column = 'date', deletion_days = 3)
