import random
import os
import pandas as pd
import psycopg2
import configparser
from datetime import timedelta, datetime

# Get the full path to the config.ini file
config = configparser.ConfigParser()
config.read("/Users/gomes/Desktop/Projects/Data Engineer/1-Project/scripts/config/config.ini")

# RDS Credentials
host = config.get("RDS", "host")
port = config.getint("RDS", "port")
dbname = config.get("RDS", "dbname")
user = config.get("RDS", "user")
password = config.get("RDS", "password")

# List of dummy data
first_names = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Joseph', 'Charles', 'Thomas']
last_names = ['Smith', 'Johnson', 'Brown', 'Williams', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Wilson']
product_names = ['Sofa', 'Armchair', 'Recliner', 'Loveseat', 'Ottoman', 'Table', 'Chair', 'Cabinet', 'Bed', 'Desk']
product_categories = ['Living Room', 'Dining Room', 'Bedroom', 'Office', 'Outdoor']
cities = ['São Paulo', 'Rio de Janeiro', 'Salvador', 'Brasília', 'Fortaleza', 'Belo Horizonte', 'Manaus', 'Curitiba', 'Recife', 'Porto Alegre']
states = ['SP', 'RJ', 'BA', 'DF', 'CE', 'MG', 'AM', 'PR', 'PE', 'RS']

def generate_data(num_customers, num_products, num_orders, num_categories, num_cities):
    # Generate category data
    categories = []
    for i in range(1, num_categories+1):
        category = {
            "CategoryID": i,
            "CategoryName": random.choice(product_categories)
        }
        categories.append(category)

    # Generate product data
    products = []
    for i in range(1, num_products+1):
        product = {
            "ProductID": i,
            "CategoryID": random.randint(1, num_categories),
            "ProductName": random.choice(product_names),
            "ProductDescription": f"This is a {random.choice(product_names)}",
            "ProductPrice": round(random.uniform(10.0, 1000.0), 2),
            "ProductInventory": random.randint(1, 100)
        }
        products.append(product)

    # Generate city data
    city_dicts = []
    for i in range(1, num_cities+1):
        city = {
            "CityID": i,
            "CityName": random.choice(cities),
            "State": random.choice(states)
        }
        city_dicts.append(city)

    # Generate customer data
    customers = []
    for i in range(1, num_customers+1):
        customer = {
            "CustomerID": i,
            "CityID": random.randint(1, num_cities),
            "FirstName": random.choice(first_names),
            "LastName": random.choice(last_names),
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
            "OrderDate": datetime.now() - timedelta(days=random.randint(1, 365)),
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
    global categories_table
    categories_table = pd.DataFrame.from_dict(categories)

    global products_table
    products_table = pd.DataFrame.from_dict(products)

    global cities_table
    cities_table = pd.DataFrame.from_dict(city_dicts)

    global customers_table
    customers_table = pd.DataFrame.from_dict(customers)

    global orders_table
    orders_table = pd.DataFrame.from_dict(orders)

    global order_items_table
    order_items_table = pd.DataFrame.from_dict(order_items)

    return "Successfully data generated"

def create_tables_postgres_and_ingest():

    # Connect to PostgreSQL RDS using psycopg2
    conn = psycopg2.connect(
        host = host,
        port = port,
        dbname = dbname,
        user = user,
        password = password
    )
    cur = conn.cursor()

    # Create the Category table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Category (
            CategoryID INT PRIMARY KEY,
            CategoryName VARCHAR(255) NOT NULL
        )
    ''')
    conn.commit()

    # Create the Product table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Product (
            ProductID INT PRIMARY KEY,
            CategoryID INT NOT NULL,
            ProductName VARCHAR(255) NOT NULL,
            ProductDescription VARCHAR(255) NOT NULL,
            ProductPrice DECIMAL(10, 2) NOT NULL,
            ProductInventory INT NOT NULL,
            FOREIGN KEY (CategoryID) REFERENCES Category(CategoryID)
        )
    ''')
    conn.commit()

    # Create the City table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS City (
            CityID INT PRIMARY KEY,
            CityName VARCHAR(255) NOT NULL,
            State VARCHAR(255) NOT NULL
        )
    ''')
    conn.commit()

    # Create the Customer Table 
    cur.execute('''
        CREATE TABLE IF NOT EXISTS Customer (
            CustomerID INT PRIMARY KEY,
            CityID INT NOT NULL,
            FirstName VARCHAR(255) NOT NULL,
            LastName VARCHAR(255) NOT NULL,
            Email VARCHAR(255) NOT NULL,
            Phone VARCHAR(20) NOT NULL,
            FOREIGN KEY (CityID) REFERENCES City(CityID)
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

    # Insert the Data in Category Table
    for index, row in categories_table.iterrows():
        cur.execute('''
            INSERT INTO Category (CategoryID, CategoryName)
            VALUES (%s, %s)
        ''', (int(row['CategoryID']), row['CategoryName']))
    
    conn.commit()

    # Insert the Data in Product Table
    for index, row in products_table.iterrows():
        cur.execute('''
            INSERT INTO Product (ProductID, CategoryID, ProductName, ProductDescription, ProductPrice, ProductInventory)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (int(row['ProductID']), int(row['CategoryID']), row['ProductName'], row['ProductDescription'], float(row['ProductPrice']), int(row['ProductInventory'])))
    
    conn.commit()

    # Insert the data in City Table
    for index, row in cities_table.iterrows():
        cur.execute('''
                        INSERT INTO City (CityID, CityName, State)
            VALUES (%s, %s, %s)
        ''', (int(row['CityID']), row['CityName'], row['State']))
    
    conn.commit()

    # Insert the data in Customers Table
    for index, row in customers_table.iterrows():
        cur.execute('''
            INSERT INTO Customer (CustomerID, CityID, FirstName, LastName, Email, Phone)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (int(row['CustomerID']), int(row['CityID']), row['FirstName'], row['LastName'], row['Email'], row['Phone']))
    
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
    cur.close()
    conn.close()




data = generate_data(50, 50, 50, 50, 50)

ingest_data = create_tables_postgres_and_ingest()

print("Data ingestion completed successfully!")












