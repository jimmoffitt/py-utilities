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


 
