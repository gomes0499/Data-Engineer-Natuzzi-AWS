import random
import pandas as pd
import boto3
import psycopg2
import json
from datetime import timedelta, datetime

def generate_data(num_customers, num_products, num_orders):
    
    # Generate product data
    products = []
    for i in range(1, num_products+1):
        product = {
            "ProductID": i,
            "ProductName": f"Product{1}",
            "ProductDescription": f"This is product {1}",
            "ProductPrice": round(random.uniform(10.0, 1000.0)),
            "ProductInventory": random.randint(1, 100)
        }
        products.append(product)

    # Generate customer data
    customers = []
    for i in range(1, num_customers+1):
        customer = {
            "CustomerID": i,
            "FirstName": f"Customer{1}",
            "LastName": "Last",
            "Email": f"customer{i}@example.com",
            "Phone": f"123-456-{i:04}"
        }
        customers.append(customer)

    # Generate order data
    orders = []
    for i in range(1, num_orders+1):
        customer_id = random.randint(1, num_customers)
        order = {
            "OrderID": i,
            "CustomerID": customer_id,
            "OrderDate": datetime.now() - timedelta(days = random.randint(1, 365)),
            "TotalAmount": 0
        }
        order_items = []
        for j in range(random.randint(1, 10)):
            product_id = random.randint(1, num_products)
            quantity = random.randint(1, 5)
            price = products[product_id-1]["ProductPrice"]
            order_item = {
                "OrderItemID": len(order_items) + 1,
                "OrderID": i,
                "ProductID": product_id,
                "Quantity": quantity,
                "Price": price
            }
            order_items.append(order_item) 
            order["TotalAmount"] += quantity * price
        orders.append(order)
    

        # Transform dict in dataframes 
        global products_table  
        products_table = pd.DataFrame.from_dict(products)

        global customers_table  
        customers_table = pd.DataFrame.from_dict(customers)

        global orders_table 
        orders_table = pd.DataFrame.from_dict(orders)

        global order_items_table  
        order_items_table = pd.DataFrame.from_dict(order_items)  
        
        # Print DataFrame
        print(products_table)
        print(customers_table)
        print(orders_table)
        print(order_items_table)

    return ("Succesfully data generated")


def create_tables_postgres_and_ingest(rds_instance_identifier = "database-1", secret_arn = "wu1postgree"):
    
    # Create RDS client and get instance details
    region = "us-east-1"
    rds = boto3.client('rds', region)
    response = rds.describe_db_instances(DBInstanceIdentifier=rds_instance_identifier)
    instance = response['DBInstances'][0]

    # Get the RDS endpoint and port
    host = instance['Endpoint']['Address']
    port = instance['Endpoint']['Port']

    # Get the RDS database name, username, and password
    secretsmanager = boto3.client("secretsmanager", region)
    secret = secretsmanager.get_secret_value(SecretId=secret_arn)
    secret_dict = json.loads(secret["SecretString"])
    dbname = secret_dict["engine"]
    user = secret_dict["username"]
    password = secret_dict["password"]

    # Connect to PostgreSQL RDS using psycopg2
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cur = conn.cursor()

    # Create the Product table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Product (
            ProductID INT PRIMARY KEY,
            ProductName VARCHAR(255) NOT NULL,
            ProductDescription VARCHAR(255) NOT NULL,
            ProductPrice DECIMAL(10, 2) NOT NULL,
            ProductInventory INT NOT NULL
        )
    ''')
    conn.commit()

    # Create the Customer Table 
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID INT PRIMARY KEY,
            FirstName VARCHAR(255) NOT NULL,
            LastName VARCHAR(255) NOT NULL,
            Email VARCHAR(255) NOT NULL,
            Phone VARCHAR(20) NOT NULL
        )
    ''')
    
    conn.commit()

    # Create the Order table 
    cur.execute('''
        CREATE TABLE IF NOT EXISTS "Order" (
            OrderID INT PRIMARY KEY,
            CustomerID INT NOT NULL,
            OrderDate DATE NOT NULL,
            TotalAmount DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)            
        )
    ''')
    
    conn.commit()

    # Create the OrderItem table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS OrderItem (
            OrderItemID INT PRIMARY KEY,
            OrderID INT NOT NULL, 
            ProductID INT NOT NULL,
            Quantity INT NOT NULL,
            Price DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (OrderID) REFERENCES "Order"(OrderID),
            FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
        )
    ''')
    
    conn.commit()

    # Insert the Data in Product Table
    for index, row in products_table.iterrows():
        cur.execute('''
            INSERT INTO Product (ProductID, ProductName, ProductDescription, ProductPrice, ProductInventory)
            VALUES (%s, %s, %s, %s, %s)
        ''', (int(row['ProductID']), row['ProductName'], row['ProductDescription'], float(row['ProductPrice']), int(row['ProductInventory'])))
    
    conn.commit()

    # Insert the data in Customers Table
    for index, row in customers_table.iterrows():
        cur.execute('''
            INSERT INTO Customer (CustomerID, FirstName, LastName, Email, Phone)
            VALUES (%s, %s, %s, %s, %s)
        ''', (int(row['CustomerID']), row['FirstName'], row['LastName'], row['Email'], row['Phone']))
    
    conn.commit()

     # Insert the data into Orders Table
    for index, row in orders_table.iterrows():
        cur.execute('''
            INSERT INTO "Order" (OrderID, CustomerID, OrderDate, TotalAmount)
            VALUES (%s, %s, %s, %s)
        ''', (int(row['OrderID']), int(row['CustomerID']), row['OrderDate'], float(row['TotalAmount'])))
    
    conn.commit()
    
    # Insert the data into Orders Items Table
    for index, row in order_items_table.iterrows():
        cur.execute('''
            INSERT INTO OrderItem (OrderItemID, OrderID, ProductID, Quantity, Price)
            VALUES (%s, %s, %s, %s, %s)
    ''', (int(row['OrderItemID']), int(row['OrderID']), int(row['ProductID']), int(row['Quantity']), float(row['Price'])))
    
    conn.commit()

    # Close the database connections
    cur.close
    conn.close

data = generate_data(50, 50, 50)
ingest_data = create_tables_postgres_and_ingest("database-1", "wu1postgree")

print("Data ingestion completed successfully!")












