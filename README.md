# py-utilities

A collection of (somewhat random) scripts written to help with demos and examples. 

## Collections
- [data-transfer](#data-transfer)
- [postgres-client](#postgres-client)
- [stress-case](#stress-case)
- [data-generators](#data-generators)
  - [e-store.py](#e-storepy)
- [converter](#converter)
- [large-files](#large-files)

## /data-transfer
A collection of scripts for moving data between Tinybird, Postgres, and DynamoDB.

To start working with (near-) real-time data sources, these began with reading from a 'live' Tinybird system and writing to a database. 

These scripts support reading data from a `source` and writing to a `target`.

Currently, these pathways are supported: 
* **Sources**: 
  * Tinybird API Endpoints. Note that Tinybird now supports the use of Postgres and DynamoDB databases as sources. 
* **Targets**:
  * Tinybird Data Sources
  * Postgres databases
  * DynamoDB databases
  * Next? MongoDB?



## /postgres-client

This project started with an exploration of using Postgres data as a Tinybird Data Source. So, a first quick step was building simple scripts for connecting and reading from a Postgres database. There are three versions, one built for version 2 of the `psycopg` Python package, one built for the [current version 3 of `psycopg`](https://www.psycopg.org/), and one that uses the [Supabase Python client](https://supabase.com/docs/reference/python/introduction). Note that these do not write data to the database, but with the underlying connection and read access establish, it should be straightforward to write  data. 
  
## /stress-case

A script built to make high-frequency requests to Tinybird and Postgres. The `stress-case.py` script was written to simulate high concurrency and its effects on performance and latency. 

This script queries a Postgres database and a Tinybird API Endpoint with common requests. Query parameters include the name of a US City and a period of interest of up to 90 days. These parameters are randomized to minimize the effects of data caching on the latency results. Data requests to both Postgres and Tinybird are made with random city names and periods. 

For a workshop demo, a Retool dashboard was built, and Retool's "success handlers" were used to measure 'round-trip' latencies. 

## /data-generators

Maybe a compilation of data generators? 

### e-store.py

Generates e-store events and streams them to Tinybird. Specifically, 'product' events that a customer can create include these types: 
* `view` 
* `cart` 
* `uncart` 
* `purchase` 
* `return`

These events are tracked by customer.

The `generate_event` function returns an event (dictionary) representing a single e-commerce event. This dictionary contains the following keys:

* `customer_id`: Unique ID of the customer involved in the event.
* `product_id`: Unique ID of the product involved in the event.
* `action`: The type of action that occurred (e.g., "view", "cart", "uncart", "purchase", "return").
* `timestamp`: The ISO-formatted timestamp of when the event happened.

As events are created, these rules are enforced:

* `cart`: A product must be viewed before it can be added to the cart.
* `uncart`: A product can only be removed from the cart if it's already in there.
* `purchase`: A product must be in the cart before it can be purchased and removed from the cart after purchase.
* `return`: A product can only be returned if it was previously purchased.

Note that if a new event does not follow these rules, it is converted to a `view` event (FWIW, the percentage of `view` events will be inflated).

The script support these settings:

```yaml
# E-commerce simulator options

# Tinybird API endpoint
tinybird_api_endpoint: "https://{CLOUD_REGION}.tinybird.co/v0/events?name={DATASOURCE_NAME}" 

# E-commerce data setup
num_customers: 500  # Number of unique customer IDs
num_products: 500    # Number of unique product IDs

# Script options
duplicate_data_percentage: 2  # Percentage of duplicate events to generate
db_update_interval_minutes: 10     # Interval (in minutes) to update totals in the database

# Enable or disable writing to Postgres
write_to_postgres: false  # Set to true to enable writing to Postgres

# Event type weights (control the percentage of each event type)
event_type_weights:
  view: 65
  cart: 15
  uncart: 5
  purchase: 10
  return: 5

```

The next step is to simulate a Postgres table getting updated every ten minutes with updated inventory numbers.

Also to-dos: 
- [ ] Support higher RPS. Make multithreaded?
- [ ] Work out the "batch inventory" Postgres details.


## /converter
A `csv-to-ndjson.py` script that converts data files from the CSV format to the ndjson format. Both types of files can be easily imported into Tinybird, but the input format choice (CSV or NDJSON) affects the auto-generated schema.  CSV files result in a schema with *explicit* Data Types, whereas NDJSON files result in a schema based on JSON parsing. If you are writing these data to a Data Source you plan on feeding with the Events API, you need to load NDJSON files for any creating and backfilling a new Data Source.  

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

 
