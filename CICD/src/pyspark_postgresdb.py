from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .config("spark.logConf", "true") \
    .enableHiveSupport() \
    .getOrCreate()

url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

#table_name = "bank"

try:
    df = spark.read.jdbc(url=url, table="bank", properties=properties)
    df.printSchema()  # Assuming this was the operation causing the issue
    df.show(5)  # Print some data to verify if DataFrame is loaded correctly
except Exception as e:
    print("Error reading data from PostgreSQL: ",e)
finally:
    spark.stop()


#connecting pyspark with postgressql
# spark-submit --jars postgresql-42.6.0.jar myspark_pgresdb.py
# spark-submit  myspark_pgresdb.py
