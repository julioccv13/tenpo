## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Context](#context)
* [Adding new properties](#steps-to-add-new-properties)

## General info
This project is design to consume all the upcoming data from clevertap deposit in our GCP cloud storage
	
## Technologies
Project is created uses:
* Pyspark: 3.2.1
* Dataproc
	
## Context
Every 4 hours we receive data from clevertap which comes in the format of json zipped files and landed in the bucket called:

    clevertap_landing

Once we receive all the data, we execute a gsutil script to divide it into daily folders and then send it to a folder called:

    clevertap_staging

After the spliting, we unzip the json files and transform it into parquet files to improve performance and make it light weight, once the transformation it's done, we finally send it to this bucket:

    clevertap_gold

## Steps to add new properties

If you want to add new properites you'll need to modify the schema in the prod.json file by adding it at the end of the file, in case it's a event property, it should go inside of the eventprops structure.

Once you done that, you'll have to modify the main.py by adding the new fields in the SCHEMA, as shown bellow:

    .add("journey_name", StringType(), True)
    .add("closemotives", StringType(), True)
    .add("closeaccountreason", StringType(), True),

Here's a few MR as example on how they modify the main.py

https://gitlab.com/tenpo/data/asgard-data-company/-/merge_requests/104/diffs

https://gitlab.com/tenpo/data/asgard-data-company/-/merge_requests/499/diffs


## Modifiying schema productive env

Once you change the prod.json file and the main.py you have to update manually the schema of the bigquery table, since it's have nested fields inside, the table can't be traditionally updated with an ALTER TABLE command, you'll have to use the cloud shell instead. Try first with the sandbox projet by executing the following command:

    bq update tenpo-datalake-sandbox:clevertap_raw.clevertap_gold_external prod.json

In order to pass the prod.json file, you'll have to upload it from your local into the cloud shell file system.

If your test in sandbox was successful, you can then executed in the prod environment:

    bq update tenpo-datalake-prod:clevertap_raw.clevertap_gold_external prod.json


## Modifiying schema in bi layer

One thing to have in mind is that this change affects directly to one of the most <b>important</b> tables in the organization, so we'll have to update it in our bi layer, that's why there's a separte json file called "prod_bi.json", the steps will be the same, go the cloud shell and execute the following command:

    bq update tenpo-bi-dev:external.clevertap_raw prod_bi.json

Once you test in dev pass, you can then submit your changes to the bi prod environment:

    bq update tenpo-bi-prod:external.clevertap_raw prod_bi.json

## Backfill

In case you have to re-execute from a certain date, you'll have to do it by using he Trigger DAG w/config option and pass int in the configuration JSON the date you want to execute it

    {"exec_date": "2023-04-12"}

