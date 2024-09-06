import random
import time
import datetime
import requests
import os
import psycopg
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env.local
script_dir = Path(__file__).parent
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# Load other options from YAML file
yaml_path = script_dir / 'settings.yaml'
with open(yaml_path, "r") as f:
    options = yaml.safe_load(f)

# Database connection details from .env.local
TINYBIRD_TARGET_TOKEN = os.getenv("TINYBIRD_TARGET_TOKEN")  # From .env.local
POSTGRES_DATABASE_HOST = os.getenv("POSTGRES_DATABASE_HOST")
POSTGRES_DATABASE_PORT = int(os.getenv("POSTGRES_DATABASE_PORT"))
POSTGRES_DATABASE_NAME = os.getenv("POSTGRES_DATABASE_NAME")
POSTGRES_DATABASE_USER = os.getenv("POSTGRES_DATABASE_USER")
POSTGRES_DATABASE_PASSWORD = os.getenv("POSTGRES_DATABASE_PASSWORD")

# Load app settings. 
TINYBIRD_API_ENDPOINT = options["tinybird_api_endpoint"]
DB_UPDATE_INTERVAL_MINUTES = options["db_update_interval_minutes"]

# E-commerce data setup
CUSTOMERS = [f"customer_{i}" for i in range(options["num_customers"])]
PRODUCTS = [
    {"id": f"product_{i}", "description": f"Product {i} description", "price": random.randint(10, 100)} 
    for i in range(options["num_products"])
]

# Internal state for carts and purchases
carts = {customer: [] for customer in CUSTOMERS}
purchases = {customer: [] for customer in CUSTOMERS}

# Script options from YAML
DUPLICATE_DATA_PERCENTAGE = options["duplicate_data_percentage"]
DB_UPDATE_INTERVAL_MINUTES = options["db_update_interval_minutes"] # Convert minutes to seconds
EVENT_TYPE_WEIGHTS = options["event_type_weights"]

# Database setup (conditional)
if options.get("write_to_postgres", False):  # Check if write_to_postgres is True
    conn = psycopg.connect(
        host=POSTGRES_DATABASE_HOST,
        port=POSTGRES_DATABASE_PORT,
        dbname=POSTGRES_DATABASE_NAME,
        user=POSTGRES_DATABASE_USER,
        password=POSTGRES_DATABASE_PASSWORD
    )
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute('''
        CREATE TABLE IF NOT EXISTS ecomm_totals (
            timestamp TIMESTAMP,
            total_orders INT,
            total_returns INT,
            total_carts INT,
            total_uncarts INT,
            total_views INT
        )
    ''')
    conn.commit()
else:
    conn = None  # Set conn to None if not writing to Postgres
    cur = None

# Internal state for carts, purchases, and viewed products
carts = {customer: [] for customer in CUSTOMERS}
purchases = {customer: [] for customer in CUSTOMERS}
viewed_products = {customer: set() for customer in CUSTOMERS}

def generate_event():
    """Generates a random e-commerce event based on specified weights and enforced rules."""
    customer = random.choice(CUSTOMERS)
    product = random.choice(PRODUCTS)

    # Calculate total weight for normalization
    total_weight = sum(EVENT_TYPE_WEIGHTS.values())

    # Normalize weights to probabilities
    probabilities = [weight / total_weight for weight in EVENT_TYPE_WEIGHTS.values()]

    # Choose an action, but may need to adjust based on rules
    potential_action = random.choices(
        list(EVENT_TYPE_WEIGHTS.keys()), weights=probabilities
    )[0]

    # Enforce the rules for event progression
    if potential_action == "cart":
        if product["id"] not in viewed_products[customer]:
            action = "view"  # Must view before adding to cart
            viewed_products[customer].add(product["id"])
        elif product["id"] in carts[customer]:
            action = "view"  # Already in cart, change to view
        else:
            action = "cart"
            carts[customer].append(product["id"])

    elif potential_action == "uncart":
        if product["id"] not in carts[customer]:
            action = "view"  # Can't remove if not in cart
        else:
            action = "uncart"
            carts[customer].remove(product["id"])

    elif potential_action == "purchase":
        if product["id"] not in carts[customer]:
            action = "view"  # Can't purchase if not in cart
        else:
            action = "purchase"
            purchases[customer].append(product["id"])
            carts[customer].remove(product["id"])  # Remove from cart after purchase

    elif potential_action == "return":
        if product["id"] not in purchases[customer]:
            action = "view"  # Can't return if not purchased
        else:
            action = "return"
            purchases[customer].remove(product["id"])

    else:  # view action or fallback
        action = "view"
        viewed_products[customer].add(product["id"])

    event = {
        "customer_id": customer,
        "product_id": product["id"],
        "action": action,
        "timestamp": datetime.datetime.now().isoformat(),
    }

    # Duplicate logic: If a duplicate is to be created, send the event twice immediately
    if random.randint(0, 100) < DUPLICATE_DATA_PERCENTAGE:
        send_event_to_tinybird(event)  # Send the soon-to-be-duplicate event immediately

    return event 

def send_event_to_tinybird(event):
    """Sends an event to the Tinybird API endpoint with authentication."""
    try:
        headers = {"Authorization": f"Bearer {TINYBIRD_TARGET_TOKEN}"}
        response = requests.post(TINYBIRD_API_ENDPOINT, json=event, headers=headers)
        response.raise_for_status() 
        print(f"Event sent to Tinybird: {event}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending event to Tinybird: {e}")

def record_totals_to_db():

    """Records internal totals to the PostgreSQL table if enabled."""
    if conn is not None:  # Only execute if connected to Postgres

        total_orders = sum(len(purchases[customer]) for customer in CUSTOMERS)
        total_returns = sum(
            len(purchases[customer]) - len(set(purchases[customer])) for customer in CUSTOMERS
        )
        total_carts = sum(len(carts[customer]) for customer in CUSTOMERS)
        total_uncarts = sum(
            len(purchases[customer]) - len(carts[customer]) for customer in CUSTOMERS
        )
        total_views = 0  # Calculate based on events if needed

        cur.execute(
            """
            INSERT INTO ecomm_totals (timestamp, total_orders, total_returns, total_carts, total_uncarts, total_views)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                datetime.datetime.now(),
                total_orders,
                total_returns,
                total_carts,
                total_uncarts,
                total_views,
            ),
        )
        conn.commit()

if __name__ == "__main__":
    last_totals_update = time.time()

    while True:
        # Create event and send to Tinybird. 
        event = generate_event()
        send_event_to_tinybird(event)

        # Record totals to the database every DB_UPDATE_INTERVAL_MINUTES converted to seconds (if enabled)
        if options.get("write_to_postgres", False) and time.time() - last_totals_update >= DB_UPDATE_INTERVAL_MINUTES * 60:
            record_totals_to_db()
            last_totals_update = time.time()

        # time.sleep(1) 

    # Close the database connection when the script ends (if it was opened)
    if conn is not None:
        cur.close()
        conn.close()