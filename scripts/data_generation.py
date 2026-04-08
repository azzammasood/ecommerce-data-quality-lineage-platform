import os
import random
from datetime import datetime
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine

fake = Faker()

def generate_users(num_users):
    users = []
    for _ in range(num_users):
        users.append({
            'user_id': fake.unique.random_int(1, 1000000),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'created_at': fake.date_time_between(start_date='-2y', end_date='now')
        })
    return pd.DataFrame(users)

def generate_products(num_products):
    products = []
    categories = ['Electronics', 'Clothing', 'Home', 'Books', 'Toys']
    for _ in range(num_products):
        products.append({
            'product_id': fake.unique.random_int(1, 1000000),
            'name': fake.word().capitalize() + ' ' + fake.word().capitalize(),
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 500.0), 2)
        })
    return pd.DataFrame(products)

def generate_orders_and_items(users_df, products_df, execution_date_str):
    exec_date = datetime.strptime(execution_date_str, '%Y-%m-%d')
    num_orders = random.randint(50, 150)
    
    orders = []
    items = []
    
    for _ in range(num_orders):
        order_id = fake.unique.random_int(1, 10000000)
        user_id = random.choice(users_df['user_id'].tolist())
        
        # Order generated on the execution day
        created_at = fake.date_time_between_dates(
            datetime.combine(exec_date, datetime.min.time()),
            datetime.combine(exec_date, datetime.max.time())
        )
        status = random.choice(['pending', 'shipped', 'delivered', 'returned', 'cancelled'])
        
        orders.append({
            'order_id': order_id,
            'user_id': user_id,
            'status': status,
            'created_at': created_at
        })
        
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_id = random.choice(products_df['product_id'].tolist())
            quantity = random.randint(1, 3)
            price = products_df[products_df['product_id'] == product_id]['price'].values[0]
            
            items.append({
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': price
            })
            
    return pd.DataFrame(orders), pd.DataFrame(items)

def main():
    # If explicitly passed via env, use it (like from Airflow), otherwise default local
    db_url = os.getenv('DATABASE_URL', 'postgresql://ecommerce_user:ecommerce_pass@localhost:5432/ecommerce')
    print(f"Connecting to {db_url}")
    engine = create_engine(db_url)
    
    # We get logical execution date from Airflow, or default to today
    execution_date_str = os.getenv('EXECUTION_DATE', datetime.now().strftime('%Y-%m-%d'))
    print(f"Generating data for logical date: {execution_date_str}")
    
    users_df = generate_users(200)
    products_df = generate_products(50)
    orders_df, items_df = generate_orders_and_items(users_df, products_df, execution_date_str)
    
    with engine.begin() as conn:
        print("Writing Users...")
        users_df.to_sql('users', conn, schema='raw', if_exists='append', index=False)
        print("Writing Products...")
        products_df.to_sql('products', conn, schema='raw', if_exists='append', index=False)
        print("Writing Orders...")
        orders_df.to_sql('orders', conn, schema='raw', if_exists='append', index=False)
        print("Writing Order Items...")
        items_df.to_sql('order_items', conn, schema='raw', if_exists='append', index=False)
        
    print("Data generation script completed metadata push to postgres.")

if __name__ == '__main__':
    main()
