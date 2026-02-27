import os
from pymongo import MongoClient
from datetime import datetime, timedelta
import random

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'asgp_test')

def main():
    print(f'Connecting to {MONGO_URI}...')
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print('MongoDB connection successful!')
    except Exception as e:
        print(f'MongoDB connection failed: {e}')
        return
    
    db = client[MONGO_DB]
    
    print('\nClearing existing collections...')
    db.orders.delete_many({})
    db.products.delete_many({})
    db.customers.delete_many({})
    print('Collections cleared')
    
    print('\nInserting customers...')
    customers = []
    regions = ['North', 'South', 'East', 'West', 'Central']
    for i in range(1, 51):
        customers.append({
            '_id': f'CUST{i:03d}',
            'name': f'Customer {i}',
            'email': f'customer{i}@example.com',
            'region': random.choice(regions),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 365)),
            'active': random.choice([True, False])
        })
    db.customers.insert_many(customers)
    print(f'Inserted {len(customers)} customers')
    
    print('\nInserting products...')
    products = []
    categories = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys']
    for i in range(1, 101):
        products.append({
            'sku': f'SKU{i:04d}',
            'name': f'Product {i}',
            'category': random.choice(categories),
            'price': round(random.uniform(10, 500), 2),
            'stock': random.randint(0, 100),
            'rating': round(random.uniform(1, 5), 1),
            'featured': random.choice([True, False])
        })
    db.products.insert_many(products)
    print(f'Inserted {len(products)} products')
    
    print('\nInserting orders...')
    orders = []
    statuses = ['pending', 'paid', 'shipped', 'delivered', 'cancelled']
    for i in range(1, 201):
        order_date = datetime.now() - timedelta(days=random.randint(1, 90))
        status = random.choice(statuses)
        orders.append({
            'order_id': f'ORD{i:05d}',
            'customer_id': f'CUST{random.randint(1, 50):03d}',
            'product_sku': f'SKU{random.randint(1, 100):04d}',
            'quantity': random.randint(1, 5),
            'total': round(random.uniform(50, 1000), 2),
            'status': status,
            'order_date': order_date,
            'shipped_date': order_date + timedelta(days=random.randint(1, 7)) if status in ['shipped', 'delivered'] else None
        })
    db.orders.insert_many(orders)
    print(f'Inserted {len(orders)} orders')
    
    print('\n' + '='*60)
    print('Data population complete!')
    print('='*60)
    print(f'  Database: {MONGO_DB}')
    print(f'  Customers: {db.customers.count_documents({})}')
    print(f'  Products: {db.products.count_documents({})}')
    print(f'  Orders: {db.orders.count_documents({})}')
    print('='*60)
    
    client.close()

if __name__ == '__main__':
    main()
