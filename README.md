# py-utilities

A collection of (somewhat random) scripts written to help with demos and examples. 

## /data-transfer
A collection of scripts for moving data between Tinybird, Postgres, and DynamoDB.

For working with (near-) real-time data sources, these started with reading from a Tinybird API Endpoint and writing to a database. 

The code here supports reading data from a `source` writing to a `target`.

Currently, these pathways are supported: 
* **Sources**: Tinybird API Endpoints. Note that Tinybird now has support for using Postgres and DynamoDB databases as sources. 
* **Targets**: Tinybird Data Sources, Postgres, and DynamoDB databases.
  * Next? MongoDB?
  
## /stress-case

A script built to make high-frequency requests to Tinybird and Postgres. The `stress-case.py` script was written to simulate high concurrency and its effects on performance and latency. 

This script queries a Postgres database and a Tinybird API Endpoint with common requests. Query parameters include the name of a US City and a period of interest of up to 90 days. These parameters are randomized to minimize the effects of data caching on the latency results. Data requests to both Postgres and Tinybird are made with random city names and periods. Using Retool "success handlers", the time to complete the round-trip query is displayed. 

## /converter
Includes a `csv-to-ndjson.py` script that converts data files from the CSV format to the ndjson format. Both types of files can be easily imported into Tinybird, but the input format choice (CSV or NDJSON) affects the auto-generated schema.  CSV files result in a schema with *explicit* Data Types, whereas NDJSON files result in a schema based on JSON parsing. If you are writing these data to a Data Source you plan on feeding with the Events API, you need to load NDJSON files for any creating and backfilling a new Data Source.  

Here is a section of a Data Source definition created by loading an NDJSON file:

```bash
`id` Int16 `json:$.id`,
`timestamp` DateTime `json:$.timestamp`,
`value` Float32 `json:$.value`
```

When created by a CSV file, there are *explicit* data types that can not accept Event API JSON payloads without some intermediary transformations.  

```bash
`id` Int16,
`timestamp` DateTime,
`value` Float32
```

## /large-files
Includes a `csv_splitter.py` script to take a very large file and split it up into a set of smaller files. As I started moving data around to new platforms, there was often some size limit to the source data files, with 100 MB files being a common upper limit. 

 
