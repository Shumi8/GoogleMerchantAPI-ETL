# Databricks notebook source
scandi_countries = ['DK', 'SE', 'NO']

# COMMAND ----------

#Blacklist categories static table
blacklisted_categories_df = spark.read.format("delta").load(GOOGLE_BLACKLISTED_CATEGORIES_BRONZE_PATH)

# COMMAND ----------

top_brands_silver_df = spark.read.table(GOOGLE_TOP_BRANDS_SILVER_TABLE) \
                                 .join(blacklisted_categories_df, 'ranking_category_path_EN', 'left_anti')
                                 
products_bronze_df = spark.read.table(GOOGLE_PRODUCTS_BRONZE_TABLE) \
                               .transform(transform_products_bronze)

top_products_inventory_bronze_df = spark.read.table(GOOGLE_TOP_PRODUCTS_INVENTORY_BRONZE_TABLE) \
                                             .transform(transform_top_products_inventory_bronze)
                                                             
top_products_silver_df = spark.read.table(GOOGLE_TOP_PRODUCTS_SILVER_TABLE) \
                                   .join(blacklisted_categories_df, 'ranking_category_path_EN', 'left_anti')

# COMMAND ----------

# get product id and brand column in df
top_products_silver_df = top_products_silver_df.join(top_products_inventory_bronze_df, 'rank_id', 'left') \
                                               .join(products_bronze_df, 'product_id', 'left')

# COMMAND ----------

product_count_df = top_products_silver_df.groupBy('ranking_country', 'google_brand_id') \
                                         .agg(F.countDistinct('product_title_name').alias('google_product_count'))

product_count_df = product_count_df.groupBy('google_brand_id') \
                                   .agg(F.max('google_product_count').alias('google_product_count'))

# COMMAND ----------

brands_df = top_products_silver_df.select('google_brand_id') \
                                  .distinct()

# get brand column in df
top_brands_silver_df = top_brands_silver_df.join(brands_df, 'google_brand_id', 'left')

# keep only brands from scandi countries and add google product count
top_brands_silver_df = top_brands_silver_df.filter(F.col('ranking_country').isin(scandi_countries)) \
                                           .join(product_count_df, 'google_brand_id', 'left')

# COMMAND ----------

top_brands_gold_df = top_brands_silver_df.distinct()

# COMMAND ----------

top_brands_gold_df.write \
                  .format('delta') \
                  .mode('overwrite') \
                  .option("mergeSchema", "true") \
                  .option('path', GOOGLE_TOP_BRANDS_GOLD_TABLE_PATH) \
                  .saveAsTable(GOOGLE_TOP_BRANDS_GOLD_TABLE)

# COMMAND ----------

# keep only scandi countries and add google product count
top_products_gold_df = top_products_silver_df.join(product_count_df, 'google_brand_id', 'left') \
                                             .filter(F.col('ranking_country').isin(scandi_countries)) \
                                             .distinct()

# COMMAND ----------

top_products_gold_df.write \
                    .format('delta') \
                    .mode('overwrite') \
                    .option("mergeSchema", "true") \
                    .option('path', GOOGLE_TOP_PRODUCTS_GOLD_TABLE_PATH) \
                    .saveAsTable(GOOGLE_TOP_PRODUCTS_GOLD_TABLE)
