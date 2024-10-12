# Databricks notebook source
def transform_top_brands_silver(df):  
  df = df.withColumn("relative_demand_min", F.col("relative_demand").getItem("min")) \
         .withColumn("relative_demand_max", F.col("relative_demand").getItem("max")) \
         .withColumn("relative_demand_bucket", F.col("relative_demand").getItem("bucket")) \
         .withColumn("previous_relative_demand_min", F.col("previous_relative_demand").getItem("min")) \
         .withColumn("previous_relative_demand_max", F.col("previous_relative_demand").getItem("max")) \
         .withColumn("previous_relative_demand_bucket", F.col("previous_relative_demand").getItem("bucket")) \
         .withColumn("ranking_category_path", F.explode(F.col("ranking_category_path"))) \
         .withColumn("ranking_category_path_EN", F.col("ranking_category_path").getItem("name")) \
         .withColumn("ranking_category_path_locale", F.col("ranking_category_path").getItem("locale"))

  # get number of levels
  number_of_levels = df.withColumn('tmp', F.size(F.split(F.col('ranking_category_path_EN'), ' > '))).groupBy().max('tmp').collect()[0][0]

  # parse into levels
  for i in range(number_of_levels):
    df = df.withColumn(f'ranking_category_path_level_{i+1}', F.split(F.col('ranking_category_path_EN'), ' > ').getItem(i))

  df = df.filter((F.col('ranking_category_path_locale') == 'en-US')) \
         .select(F.col("rank_timestamp"), F.col("rank_id"), F.col("rank"), F.col("previous_rank"), F.col("ranking_country"), F.col("ranking_category"), F.col("brand"), F.col("google_brand_id"), F.col("_PARTITIONTIME").alias("partition_time"), F.col("_PARTITIONDATE").alias("partition_date"), F.col("ranking_category_path_EN"), F.col("ranking_category_path_level_1"), F.col("ranking_category_path_level_2"), F.col("ranking_category_path_level_3"), F.col("ranking_category_path_level_4"), F.col("ranking_category_path_level_5"), F.col("ranking_category_path_level_6"), F.col("ranking_category_path_level_7"), F.col("relative_demand_min"), F.col("relative_demand_max"), F.col("relative_demand_bucket"), F.col("previous_relative_demand_min"), F.col("previous_relative_demand_max"), F.col("previous_relative_demand_bucket"))

  return df

# COMMAND ----------

def transform_top_products_silver(df):  
  df = df.withColumn("product_title", F.explode(F.col("product_title"))) \
         .withColumn("product_title_locale", F.col("product_title").getItem("locale")) \
         .withColumn("product_title_name", F.col("product_title").getItem("name")) \
         .withColumn("relative_demand_min", F.col("relative_demand").getItem("min")) \
         .withColumn("relative_demand_max", F.col("relative_demand").getItem("max")) \
         .withColumn("relative_demand_bucket", F.col("relative_demand").getItem("bucket")) \
         .withColumn("previous_relative_demand_min", F.col("previous_relative_demand").getItem("min")) \
         .withColumn("previous_relative_demand_max", F.col("previous_relative_demand").getItem("max")) \
         .withColumn("previous_relative_demand_bucket", F.col("previous_relative_demand").getItem("bucket")) \
         .withColumn("price_range_min", F.col("price_range").getItem("min")) \
         .withColumn("price_range_max", F.col("price_range").getItem("max")) \
         .withColumn("price_range_currency", F.col("price_range").getItem("currency"))

  category_columns = ['ranking_category_path', 'google_product_category_path']

  for col in category_columns:
    # extract english category names
    df = df.withColumn(f'{col}_EN', F.col(f'{col}.name')[F.array_position(f"{col}.locale", 'en-US')-1])

    # get number of levels
    number_of_levels = df.withColumn('tmp', F.size(F.split(F.col(f'{col}_EN'), ' > '))).groupBy().max('tmp').collect()[0][0]

    # parse into levels
    for i in range(number_of_levels):
      df = df.withColumn(f'{col}_level_{i+1}', F.split(F.col(f'{col}_EN'), ' > ').getItem(i))

  df = df.select(F.col("rank_timestamp"), F.col("rank_id"), F.col("rank"), F.col("previous_rank"), F.col("ranking_country"), F.col("ranking_category"), F.col("brand"), F.col("google_brand_id"), F.col("google_product_category"), F.col("_PARTITIONTIME").alias("partition_time"), F.col("_PARTITIONDATE").alias("partition_date"), F.col("product_title_locale"), F.col("ranking_category_path_EN"), F.col("ranking_category_path_level_1"), F.col("ranking_category_path_level_2"), F.col("ranking_category_path_level_3"), F.col("ranking_category_path_level_4"), F.col("ranking_category_path_level_5"), F.col("ranking_category_path_level_6"), F.col("ranking_category_path_level_7"), F.col("google_product_category_path_EN"), F.col("google_product_category_path_level_1"), F.col("google_product_category_path_level_2"), F.col("google_product_category_path_level_3"), F.col("google_product_category_path_level_4"), F.col("google_product_category_path_level_5"), F.col("google_product_category_path_level_6"), F.col("google_product_category_path_level_7"), F.col("product_title_name"), F.col("relative_demand_min"), F.col("relative_demand_max"), F.col("relative_demand_bucket"), F.col("previous_relative_demand_min"), F.col("previous_relative_demand_max"), F.col("previous_relative_demand_bucket"), F.col("price_range_min"), F.col("price_range_max"), F.col("price_range_currency"))
  
  return df

# COMMAND ----------

def transform_products_bronze(df):

  df = df.drop("product_id") \
         .withColumnRenamed('offer_id', "product_id") \
         .withColumnRenamed("brand", "brand_name") \
         .select(F.col("product_id"), F.col("brand_name")) \
         .distinct()

  return df

# COMMAND ----------

def transform_top_products_inventory_bronze(df):
  df = df.withColumn('product_id', F.split(F.col('product_id'), ':')) \
         .withColumn('product_id', F.col("product_id").getItem(3)) \
         .select(F.col('product_id'), F.col('rank_id')) \
         .distinct()
         
  return df

# COMMAND ----------

def transform_page_feeds_silver(df):
  df = df.withColumnRenamed('date', 'full_date') \
         .withColumnRenamed('page_brand', 'page_brand_name') \
         .withColumnRenamed('page_brand_id', 'page_brand_number') \
         .withColumnRenamed('page_item_id', 'page_product_id') \
         .withColumnRenamed('feeding_page_brand', 'feeding_page_brand_name') \
         .withColumnRenamed('feeding_page_brand_id', 'feeding_page_brand_number') \
         .withColumnRenamed('feeding_page_item_id', 'feeding_page_product_id')
  
  return df

# COMMAND ----------

def transform_page_metrics_silver(df):
  df = df.withColumnRenamed('date', 'full_date') \
         .withColumnRenamed('page_brand', 'brand_name') \
         .withColumnRenamed('page_brand_id', 'brand_number') \
         .withColumnRenamed('item_id', 'product_id')
  
  return df

# COMMAND ----------

def transform_page_referrer_silver(df):
  df = df.withColumnRenamed('date', 'full_date') \
         .withColumnRenamed('page_brand', 'brand_name') \
         .withColumnRenamed('page_brand_id', 'brand_number') \
         .withColumnRenamed('item_id', 'product_id')
  
  return df
