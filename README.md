# AWS_Youtube_Data_Pipeline
YouTube Data Pipeline Project Overview:

Data Collection: Used YouTube Data API v3 to extract metadata for the top 50 trending videos across multiple regions.
Data Transformation: Applied transformation logic to clean and standardize video and channel data for warehouse ingestion.
Data Storage: Uploaded transformed datasets to structured S3 buckets for organized storage.
Redshift Integration: Created staging tables and implemented a comprehensive data model in Redshift, showcasing SCD Type 2 for the dimension_channel table to manage historical changes.
ETL Automation: Orchestrated the pipeline using AWS Step Functions and Lambda, automating data extraction, transformation, and loading into Redshift.
Outcome: Fully automated pipeline enabling efficient data flow from source to Redshift, supporting advanced analytical queries.





