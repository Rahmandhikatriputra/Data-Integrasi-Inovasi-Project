#import libraries
import psycopg
import pandas as pd
import os


#create data ingestion class

class DataIngestion:
    def __init__(self, data_source_dir, database_name, user_database, password_database, host_database, port_database):
        self.data_source_dir = data_source_dir
        self.database_name = database_name
        self.user_database = user_database
        self.password_database = password_database
        self.host_database = host_database
        self.port_database = port_database

#create method to create schema
    def create_schema(self):
        with psycopg.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            CREATE SCHEMA IF NOT EXISTS raw;
                            CREATE SCHEMA IF NOT EXISTS staging;
                            CREATE SCHEMA IF NOT EXISTS model;

                            CREATE TABLE IF NOT EXISTS raw.master_data(
                                transaction_id text,
                                customer_id text,
                                product_id text,
                                timestamp text,
                                quantity text,
                                product_name text,
                                product_category text,
                                price text,
                                join_date text,
                                customer_location text,
                                total_price text
                            );

                            CREATE TABLE IF NOT EXISTS staging.transformed_master_data(
                                transaction_id text,
                                customer_id text,
                                product_id text,
                                timestamp timestamp,
                                quantity integer,
                                product_name text,
                                product_category text,
                                price real,
                                join_date date,
                                customer_location text,
                                total_price real
                            );

                            CREATE TABLE IF NOT EXISTS model.best_selling_products(
                                product_name text,
                                total_price real
                            );
                            CREATE TABLE IF NOT EXISTS model.best_spending_customers(
                                customer_id text,
                                total_spend real
                            );

                            CREATE TABLE IF NOT EXISTS model.total_monthly_revenue(
                                year text,
                                month text,
                                total_price real
                            );
                            """)
                
#create a method to ingest data into postgresql database
    def data_ingestion(self):
        master_data = pd.read_csv(os.path.join(self.data_source_dir, "master_data.csv"))
        with psycopg.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
                    with conn.cursor() as cur:
                        # clear the table before data ingestion
                        cur.execute("""TRUNCATE raw.master_data;""")

                        # begin to data ingestion to Staging Schema
                        insert_query =  """INSERT INTO raw.master_data (
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
                                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                                        """
                        cur.executemany(insert_query, master_data.values.tolist())