#import library
import polars as pl
from script.data_preparation import DataPreparation
from script.data_ingestion import DataIngestion
from script.data_transformation import DataTransformation

def main():
#data source directory
    data_source = "data"

# define the credential of the database
    database_name = "dii_project"
    user_database = "postgres"
    password_database = "postgres"
    host_database = "localhost"
    port_database = "5432"

#execute code
    data_preparation = DataPreparation(data_source)
    data_preparation.transform()

    data_ingestion = DataIngestion(data_source, database_name, user_database, password_database, host_database, port_database)
    data_ingestion.create_schema()
    data_ingestion.data_ingestion()

    data_transformation = DataTransformation(database_name, user_database, password_database, host_database, port_database)
    data_transformation.transformation()

if __name__ == "__main__":
    main()


