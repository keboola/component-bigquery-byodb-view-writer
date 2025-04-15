BigQuery BYODB View Writer

## About

BigQuery BYODB View Writer is a component that allows Keboola developers to create authorized views on tables in Keboola Storage and propagate them to target datasets in BigQuery. This simplifies data integration between Keboola and Google BigQuery.

## Features

- **Authorized Views:** Creates and updates authorized views in target BigQuery datasets
- **Column Filtering:** Select specific columns from source tables to include in the view
- **Permission Management:** Automatically sets required permissions between sources and targets
- **User Interface:** Provides an intuitive interface for selecting sources and destinations

## Requirements

The component requires a Google Cloud Service Account with appropriate permissions:

**For source project:**
- bigquery.user role with permissions to get/update datasets and get/list tables

**For destination project:**
- bigquery.user role with permissions to create/get/update tables

Source and target datasets must be in the same Google Cloud region.

For more information about authorized views, visit [Google Cloud documentation](https://cloud.google.com/bigquery/docs/authorized-views).