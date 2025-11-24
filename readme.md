# Project Structure & File Descriptions

This document outlines the purpose of each file in the project.

---

## **`Cloud_Functions_Option_01/main.py`**

The complete ETL pipeline that extracts EV point data from the Google Places API and enriches the data using Open Charge Map.


## **`Cloud_Functions_Option_02/main.py`**

The complete ETL pipeline that extracts EV point data using only Open Charge Map.


## **`data_extracting_OCM_GPA.py`**

This notebook demonstrates how to use Google Places API location data and Open Charge Map to extract EV charging points in Central London through a local environment (without the ETL pipeline).


## **`data_cleaning_OCM_GPA.py`**

This notebook demonstrates the data-cleaning steps applied to the EV charging point data extracted using the Google Places API and Open Charge Map.


## **`data_extracting_cleaning_OCM.py`**

This notebook demonstrates how to use only the Open Charge Map data to extract EV charging points in Central London through a local environment (without the ETL pipeline). The data cleaning part also included here.


## **`Bounding-Box.ipynb`**

This notebook includes basic analyses (duplicate checking, missing value checks, etc.) along with a visualization of the bounding box used for the analysis related to Central London