# STEDI-Human-Balance-Analytics

# Project Overview
The STEDI Human Balance Analytics project focuses on building a data pipeline to analyze human balance data collected from the STEDI device. The goal is to process raw sensor data, clean and filter it, and produce curated datasets that can be used by data scientists to train machine learning models.

This project demonstrates how to design and implement a **data lake architecture** using AWS services and follow best practices for data engineering.

## Architecture Overview
The project follows a layered data architecture:

- **Landing Zone** – Raw data ingestion
- **Trusted Zone** – Cleaned and validated data
- **Curated Zone** – Analytics-ready datasets

Data is stored and processed using AWS services such as S3 Bucket, Glue Studio, and Athena.
