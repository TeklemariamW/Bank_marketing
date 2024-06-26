#import findspark
#findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgreSQL Connection with PySpark") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

table_name = "bank"

#df = spark.read.jdbc(url, table_name, properties=properties)
try:
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    df.show()
except Exception as e:
    print("Error reading data from PostgreSQL: ", e)


# Show DataFrame Schema
df.printSchema()

# Show First 5 Rows
df.show(5)

#connecting pyspark with postgressql
# spark-submit --jars postgresql-42.6.0.jar myspark_pgresdb.py
# spark-submit  myspark_pgresdb.py
