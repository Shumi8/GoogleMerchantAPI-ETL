# Databricks notebook source
def perform_silver_ingestion(BRONZE_TABLE, SILVER_TABLE_PATH, SILVER_TABLE, transform_function):

  bronze_df = spark.read \
                   .table(BRONZE_TABLE) \
                   .transform(transform_function)

  bronze_df.write \
           .format('delta') \
           .mode('overwrite') \
           .option('mergeSchema', 'true') \
           .option('path', SILVER_TABLE_PATH) \
           .saveAsTable(SILVER_TABLE)

# COMMAND ----------

print("writing top brands silver table")
perform_silver_ingestion(BRONZE_TABLE = GOOGLE_TOP_BRANDS_BRONZE_TABLE, SILVER_TABLE_PATH = GOOGLE_TOP_BRANDS_SILVER_TABLE_PATH, SILVER_TABLE = GOOGLE_TOP_BRANDS_SILVER_TABLE, transform_function = transform_top_brands_silver)

print("writing top products silver table")
perform_silver_ingestion(BRONZE_TABLE = GOOGLE_TOP_PRODUCTS_BRONZE_TABLE, SILVER_TABLE_PATH = GOOGLE_TOP_PRODUCTS_SILVER_TABLE_PATH, SILVER_TABLE = GOOGLE_TOP_PRODUCTS_SILVER_TABLE, transform_function = transform_top_products_silver)

print("writing page feeds silver table")
perform_silver_ingestion(BRONZE_TABLE = GOOGLE_PAGE_FEEDS_BRONZE_TABLE, SILVER_TABLE_PATH = GOOGLE_PAGE_FEEDS_SILVER_TABLE_PATH, SILVER_TABLE = GOOGLE_PAGE_FEEDS_SILVER_TABLE, transform_function = transform_page_feeds_silver)

print("writing page metrics silver table")
perform_silver_ingestion(BRONZE_TABLE = GOOGLE_PAGE_METRICS_BRONZE_TABLE, SILVER_TABLE_PATH = GOOGLE_PAGE_METRICS_SILVER_TABLE_PATH, SILVER_TABLE = GOOGLE_PAGE_METRICS_SILVER_TABLE, transform_function = transform_page_metrics_silver)

print("writing page referrer silver table")
perform_silver_ingestion(BRONZE_TABLE = GOOGLE_PAGE_REFERRER_BRONZE_TABLE, SILVER_TABLE_PATH = GOOGLE_PAGE_REFERRER_SILVER_TABLE_PATH, SILVER_TABLE = GOOGLE_PAGE_REFERRER_SILVER_TABLE, transform_function = transform_page_referrer_silver)
