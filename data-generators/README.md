
### e-store.py

Generates e-store events and streams them to Tinybird. Specifically, 'product' events that a customer can create include these types: 
* `view` 
* `cart` 
* `uncart` 
* `purchase` 
* `return`

The `generate_event` function returns an event (dictionary) representing a single e-commerce event. This dictionary contains the following keys:

* `customer_id`: Unique ID of the customer involved in the event.
* `product_id`: Unique ID of the product involved in the event.
* `action`: The type of action that occurred (e.g., "view", "cart", "uncart", "purchase", "return").
* `timestamp`: The ISO-formatted timestamp of when the event happened.

The script tracks these events by customer and retains event history to enforce 'life-cycle' rules. 

As events are created, these rules are enforced:

* `cart`: A product must be viewed before it can be added to the cart.
* `uncart`: A product can only be removed from the cart if it's already in there.
* `purchase`: A product must be in the cart before it can be purchased and removed from the cart after purchase.
* `return`: A product can only be returned if it was previously purchased.

Note that if a new event does not follow these rules, it is converted to a `view` event (FWIW, the percentage of `view` events will be inflated).

The script supports these settings:

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
