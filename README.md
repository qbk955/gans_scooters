# Cloud-Based Data Pipeline: From Web Scraping to GCP

## Introduction

This project demonstrates the process of building a cloud-based data pipeline, starting with web scraping using Python and ending with the data being processed and stored in Google Cloud Platform (GCP). The goal of the pipeline is to automate the collection and transformation of data before storing it in a cloud-based environment for further analysis and visualization.

The full explanation of the project is detailed in the [Medium article](https://medium.com/@jakubczerniawski/building-a-cloud-based-data-pipeline-from-web-scraping-through-python-to-gcp-b48f96f6f175).

## Project Overview

### Steps Involved

1. **Web Scraping**:
   - Scraping data from the web using Python's `requests` and `BeautifulSoup`.
   - Parsing the scraped HTML content to extract relevant information.
   
2. **Data Cleaning and Transformation**:
   - Using `pandas` for data cleaning and transformation.
   - Ensuring the data is in a structured format suitable for further processing.

3. **Data Storage in Google Cloud**:
   - Setting up Google Cloud Storage (GCS) for data storage.
   - Using GCP’s services (like BigQuery) for data processing and querying.
   
4. **Automation and Scheduling**:
   - Using tools such as `cron` or Cloud Functions to automate the scraping and data pipeline process on a regular basis.



