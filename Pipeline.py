# COMMAND ----------

# MAGIC %run ../../Utilities/Configuration

# COMMAND ----------

# MAGIC %run ./Utilities/DataTransformation

# COMMAND ----------

top_brands_table = "your_database.your_schema.top_brands"
top_products_table = "your_database.your_schema.top_products"
top_products_inventory_table = "your_database.your_schema.top_products_inventory"
products_table = "your_database.your_schema.products"
products_price_benchmarks_table = "your_database.your_schema.products_price_benchmarks"
page_feeds_table = "your_database.your_schema.page_feeds"
page_metrics_table = "your_database.your_schema.page_metrics"
page_referrer_table = "your_database.your_schema.page_referrer"

# COMMAND ----------

GOOGLE_TOP_BRANDS_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/xxxx/TopBrands/'
GOOGLE_TOP_BRANDS_BRONZE_TABLE = 'your_database.google_merchant.top_brands_bronze'

GOOGLE_TOP_PRODUCTS_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/xxxx/TopProducts/'
GOOGLE_TOP_PRODUCTS_BRONZE_TABLE = 'your_database.google_merchant.top_products_bronze'

GOOGLE_TOP_PRODUCTS_INVENTORY_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/xxxx/TopProductsInventory/'
GOOGLE_TOP_PRODUCTS_INVENTORY_BRONZE_TABLE = 'your_database.google_merchant.top_products_inventory_bronze'

GOOGLE_PRODUCTS_PRICE_BENCHMARKS_BRONZE_TABLE_PATH = src_string+'/Bronze/Google/GoogleMerchant/PriceBenchmarks/'
GOOGLE_PRODUCTS_PRICE_BENCHMARKS_BRONZE_TABLE = 'your_database.google_merchant.price_benchmarks_bronze'

GOOGLE_PRODUCTS_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/xxxx/Products/'
GOOGLE_PRODUCTS_BRONZE_TABLE = 'your_database.google_merchant.products_bronze'

GOOGLE_BLACKLISTED_CATEGORIES_BRONZE_PATH = src_string+'/xxxx/xxxx/GoogleMerchant/Blacklisted/'

GOOGLE_PAGE_FEEDS_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/PageFeeds/'
GOOGLE_PAGE_FEEDS_BRONZE_TABLE = 'your_database.google.page_feeds_bronze'

GOOGLE_PAGE_METRICS_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/PageMetrics/'
GOOGLE_PAGE_METRICS_BRONZE_TABLE = 'your_database.google.page_metrics_bronze'

GOOGLE_PAGE_REFERRER_BRONZE_TABLE_PATH = src_string+'/xxxx/xxxx/PageReferrer/'
GOOGLE_PAGE_REFERRER_BRONZE_TABLE = 'your_database.google.page_referrer_bronze'

# COMMAND ----------

GOOGLE_TOP_BRANDS_SILVER_TABLE_PATH = src_string+'/xxxx/xxxx/TopBrands/'
GOOGLE_TOP_BRANDS_SILVER_TABLE = 'your_database.google_merchant.top_brands_silver'

GOOGLE_TOP_PRODUCTS_SILVER_TABLE_PATH = src_string+'/xxxx/xxxx/TopProducts/'
GOOGLE_TOP_PRODUCTS_SILVER_TABLE = 'your_database.google_merchant.top_products_silver'

GOOGLE_PAGE_FEEDS_SILVER_TABLE_PATH = src_string+'/xxxx/xxxx/PageFeeds/'
GOOGLE_PAGE_FEEDS_SILVER_TABLE = 'your_database.google.page_feeds_silver'

GOOGLE_PAGE_METRICS_SILVER_TABLE_PATH = src_string+'/xxxx/xxxx/PageMetrics/'
GOOGLE_PAGE_METRICS_SILVER_TABLE = 'your_database.google.page_metrics_silver'

GOOGLE_PAGE_REFERRER_SILVER_TABLE_PATH = src_string+'/xxxx/xxxx/PageReferrer/'
GOOGLE_PAGE_REFERRER_SILVER_TABLE = 'your_database.google.page_referrer_silver'

# COMMAND ----------

GOOGLE_TOP_BRANDS_GOLD_TABLE_PATH = src_string+'/xxxx/xxxx/TopBrands/'
GOOGLE_TOP_BRANDS_GOLD_TABLE = 'your_database.google_merchant.top_brands_gold'

GOOGLE_TOP_PRODUCTS_GOLD_TABLE_PATH = src_string+'/xxxx/xxxx/TopProducts/'
GOOGLE_TOP_PRODUCTS_GOLD_TABLE = 'your_database.google_merchant.top_products_gold'

# COMMAND ----------

PRODUCT_TABLE_PATH = src_string+'/xxxx/xxxx/xxxx/xxxx/GoldProductDim/'

# COMMAND ----------

# MAGIC %run ./RawToBronze/Pipeline

# COMMAND ----------

# MAGIC %run ./BronzeToSilver/Pipeline

# COMMAND ----------

# MAGIC %run ./SilverToGold/Pipeline
