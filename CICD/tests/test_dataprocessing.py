import pandas as pd
from src.createCICD import process_data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.appName("Testing PySpark").getOrCreate()

def test_process_data():

    sample_data = [{"name": "John    D.", "age": 30},
            {"name": "Alice   G.", "age": 25},
            {"name": "Bob  T.", "age": 35},
            {"name": "Eve   A.", "age": 28}]
    
    # Create a sample DataFrame
    df = spark.createDataFrame(sample_data)
    
    # Save the DataFrame to a CSV file
    df.to_csv('test_data.csv', index=False)
    
    # Call the process_data function
    result = process_data('test_data.csv')
    
    # Check the result
    assert result == 11  # Sum of non-missing values in the 'column' column

if __name__ == "__main__":
   
    test_process_data()
