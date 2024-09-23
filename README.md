BigQuery BYODB View Writer
=============

This component allows Keboola developers to create authorized views on the tables in Keboola storage and propagate them to the target datasets in BQ (BYODB). [More about Authorized Views here](https://cloud.google.com/bigquery/docs/authorized-views)


Functionality notes
===================

It is possible to define the name of the view and only some columns can be selected.

Prerequisites
=============

Component needs the Google Cloud [Service account](https://cloud.google.com/iam/docs/service-accounts-create#console) (as JSON) with the following roles:
- For the source project: 
    - role bigquery.user
    - and the following permissions:
        - bigquery.datasets.get
        - bigquery.datasets.update
        - bigquery.tables.get
        - bigquery.tables.getData
        - bigquery.tables.list
- For the destination project:
  - role bigquery.user
  - and the following permissions:
      - bigquery.tables.create
      - bigquery.tables.get
      - bigquery.tables.update


Configuration
=============

Configuration paramaters:
-------
- Service Account - JSON
  - BigQuery service account key in JSON format
- Source Project
  - Keboola Project ID ("sapi-XXXX-ZZZZ")
    - BigQuery projects is created while creating the new Keboola project, it is formed by combining the project number and a random string. The component does not have chance to a find it, unfortunately.
- Destination Project
  - Destination BigQuery project id
- Destination Dataset
  - Destination BigQuery dataset id

Configuration row parameters:
-------
- Destination View Name
  - Destination BigQuery view name
- Source Bucket
  - Source Keboola storage bucket 
- Source Table
  - Source Keboola storage table
- Custom columns
  - Allow filtering of columns from the source table

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-bigquery-byodb-view-writer bigquery_byodb_view_writer
cd bigquery_byodb_view_writer
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
