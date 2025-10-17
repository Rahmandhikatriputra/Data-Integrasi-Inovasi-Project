#import libraries
import psycopg
import pandas as pd
import os


#create data transformation class

class DataTransformation:
    def __init__(self, database_name, user_database, password_database, host_database, port_database):
        self.database_name = database_name
        self.user_database = user_database
        self.password_database = password_database
        self.host_database = host_database
        self.port_database = port_database

#create a method to start transformation task
    def transformation(self):
        with psycopg.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            TRUNCATE staging.transformed_master_data;

                            INSERT INTO staging.transformed_master_data(
                                transaction_id,
                                customer_id,
                                product_id,
                                timestamp,
                                quantity,
                                product_name,
                                product_category,
                                price,
                                join_date,
                                customer_location,
                                total_price
                            )
                            SELECT transaction_id, 
                                    customer_id, 
                                    product_id, 
                                    TO_TIMESTAMP(timestamp, 'MM/DD/YYYY HH24:MI'),
                                    CAST(quantity AS INTEGER), 
                                    product_name, 
                                    product_category, 
                                    CAST(price AS REAL), 
                                    TO_DATE(join_date, 'YYYY-MM-DD'), 
                                    customer_location,
                                    CAST(total_price AS REAL)
                            FROM raw.master_data;

                            INSERT INTO model.best_selling_products(
                            product_name,
                            total_price
                            )
                            SELECT product_name, 
                                SUM(total_price) AS total_price
                            FROM staging.transformed_master_data
                            GROUP BY product_name
                            ORDER BY total_price DESC
                            LIMIT 5;
                            
                            INSERT INTO model.best_spending_customers(
                            customer_id,
                            total_spend
                            )
                            SELECT customer_id, 
                                SUM(total_price) AS total_spend
                            FROM staging.transformed_master_data
                            GROUP BY customer_id
                            ORDER BY total_spend DESC
                            LIMIT 10;

                            INSERT INTO model.total_monthly_revenue(
                                year,
                                month,
                                total_price 
                            )
                            SELECT EXTRACT(YEAR FROM timestamp) AS year,
                                EXTRACT(MONTH FROM timestamp) AS month,
                                SUM(total_price) AS total_price
                            FROM staging.transformed_master_data
                            GROUP BY month, year
                            ORDER BY year, month;
                            """)
                