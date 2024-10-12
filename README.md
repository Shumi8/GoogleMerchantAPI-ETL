# ETL Pipeline for Google Merchant Data in Retail Organization

This repository contains an **ETL pipeline** developed for Denmark's largest retail organization to process **Google Merchant data** on Databricks. The pipeline extracts, transforms, and loads (ETL) various data sources such as top brands, top products, price benchmarks, and page metrics across multiple layers (Raw to Bronze, Bronze to Silver, Silver to Gold). It enables insights into product demand, brand performance, and other key business metrics.

## Features

- **Multi-Stage Data Processing**: The pipeline follows a three-layer architecture — **Raw to Bronze**, **Bronze to Silver**, and **Silver to Gold** for optimal data transformation and storage.
- **Environment-Specific Configurations**: Uses Databricks widgets for dynamic configuration of environments, allowing users to switch between development and production settings.
- **Google Merchant Data Integration**: Ingests data from Google Merchant datasets, including top brands, top products, inventory, price benchmarks, page feeds, and metrics.
- **Data Transformation**: Implements complex transformations for cleaning, enriching, and preparing data at each stage of the pipeline.
- **Data Aggregation**: Aggregates data across different dimensions like product and brand to produce insights that are later used in Gold-level datasets.

## Technologies Used

### 1. **Databricks Notebooks**
- The pipeline is built entirely in **Databricks notebooks**, allowing for easy management of data transformation logic using PySpark and Delta Lake.

### 2. **Delta Lake**
- Delta Lake is used to store and manage the datasets at each layer (Bronze, Silver, and Gold). It provides efficient data storage, versioning, and ACID transactions.

### 3. **BigQuery**
- The pipeline interacts with **Google BigQuery** to read data from the source tables for ingestion into Databricks.

## Pipeline Workflow

### 1. **Raw to Bronze**
- **File**: `RawToBronze/Pipeline.py`
- **Function**: Reads data from **BigQuery** tables and writes the ingested data into **Bronze Delta Tables**. It performs a "delete and insert" operation, ensuring data freshness by deleting older records and ingesting new ones.
  
### 2. **Bronze to Silver**
- **File**: `BronzeToSilver/Pipeline.py`
- **Function**: Reads data from the **Bronze Delta Tables** and applies transformations, cleaning the data for further processing. This step includes joins with reference datasets and other complex transformations.

### 3. **Silver to Gold**
- **File**: `SilverToGold/Pipeline.py`
- **Function**: Aggregates the **Silver Delta Tables** and produces final tables with the cleaned, transformed, and aggregated data. These **Gold Delta Tables** are used for reporting and analysis purposes.

## Data Processing Details

### 1. **Google Merchant Data**
- **Top Brands**: Processes top brands data by ranking them based on demand and performance.
- **Top Products**: Extracts top-performing products and tracks their inventory.
- **Price Benchmarks**: Processes pricing data and compares product prices across different benchmarks.
- **Page Feeds & Metrics**: Ingests and transforms data about page views, metrics, and referrals to assess website performance.

### 2. **Data Transformation Functions**
- **File**: `DataTransformation.py`
- **Function**: Contains a variety of helper functions for transforming data, such as parsing product categories, demand metrics, and product inventory details.

## Configuration

- **Pipeline.py**: Configures the environment settings and paths for different datasets, enabling seamless switching between **Development** and **Production** environments.
- **Databricks Widgets**: Interactive widgets are used to allow users to dynamically select environments and data access levels (GDPR-Restricted or Unrestricted).

## Usage

1. **Run the Pipeline**: 
   - The pipeline can be executed in Databricks notebooks, starting from `Pipeline.py`.
   - The widgets allow users to select the environment and the access level dynamically.

2. **Data Transformation Stages**:
   - Each stage of the pipeline (Raw to Bronze, Bronze to Silver, and Silver to Gold) is implemented as separate notebooks, allowing for modular execution and debugging.

3. **Data Storage**:
   - Data is stored in Delta format at each stage for efficient querying and storage management.
