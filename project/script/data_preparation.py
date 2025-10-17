import polars as pl
import os

#create a data preparation class
class DataPreparation:
    def __init__(self, data_source_dir):
        self.data_source_dir = data_source_dir

#create a transform function
    def transform(self):

#read the data sources    
        customer_data = pl.read_csv(os.path.join(self.data_source_dir, "customers.csv"))
        product_data = pl.read_csv(os.path.join(self.data_source_dir, "products.csv"))
        transaction_data = pl.read_csv(os.path.join(self.data_source_dir, "transactions.csv"))

#merge dataset
        first_merge = transaction_data.join(product_data, on = "product_id")
        master_dataset = first_merge.join(customer_data, on = "customer_id")

#add new column for total price
        master_dataset = master_dataset.with_columns((pl.col("quantity") * pl.col("price")).alias("total_price"))

#standardize the timestamp data into appropriate format
        def standardize_timestamp(x):
                first = x.split(" ")[0]
                second = x.split(" ")[1]

                day = int(first.split("/")[0])
                day = f"{day:02d}"

                month = int(first.split("/")[1])
                month = f"{month:02d}"

                year = int(first.split("/")[2])

                return f"{day}/{month}/{year} {second}"

        master_dataset = master_dataset.with_columns(pl.col("timestamp").map_elements(standardize_timestamp))        

#delete null values in total_price column
        master_dataset = master_dataset.drop_nulls(subset = ["total_price"])
        
#convert dataset into csv file
        master_dataset.write_csv(os.path.join(self.data_source_dir, "master_data.csv"))


        # test = pd.read_parquet(os.path.join(self.dir_input, list_dataset))