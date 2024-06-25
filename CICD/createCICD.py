#pyspark
#from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, upper, concat, length, current_date, sum

# Initialize Spark Session
spark = SparkSession.builder.appName("CSV Transformations").getOrCreate()

# Read CSV File
df = spark.read.csv("/tmp/catbd125/Tekle/student.csv", header=True, inferSchema=True)

# Show DataFrame Schema
df.printSchema()

# Show First 5 Rows
df.show(5)

# Filter Rows Where Country is 'Cyprus'
df_cyprus = df.filter(df['country'] == 'Cyprus')
df_cyprus.show()

# Select Specific Columns
df_select = df.select("id", "firstname", "lastname")
df_select.show()

# Add a New Column with Constant Value
df_with_constant = df.withColumn("new_column", lit("constant_value"))
df_with_constant.show()

# Rename a Column
df_renamed = df.withColumnRenamed("phonenumber", "phone_number")
df_renamed.show()

# Drop a Column
df_dropped = df.drop("country")
df_dropped.show()

# Group By Country and Count
df_grouped = df.groupBy("country").count()
df_grouped.show()

# Order By First Name
df_ordered = df.orderBy("firstname")
df_ordered.show()

# Filter Rows Where Phone Number is Greater Than 50000000
df_filtered = df.filter(df['phonenumber'] > 50000000)
df_filtered.show()

# Convert First Name to Upper Case
df_upper = df.withColumn("firstname_upper", upper(df['firstname']))
df_upper.show()

# Concatenate First Name and Last Name
df_concat = df.withColumn("full_name", concat(df['firstname'], lit(" "), df['lastname']))
df_concat.show()

# Remove Duplicate Rows
df_distinct = df.dropDuplicates()
df_distinct.show()

# Calculate Length of First Name
df_length = df.withColumn("firstname_length", length(df['firstname']))
df_length.show()

# Replace Null Values in a Column
df_filled = df.na.fill({"phonenumber": 0})
df_filled.show()

# Filter Rows Based on Length of Last Name
df_filtered_length = df.filter(length(df['lastname']) > 5)
df_filtered_length.show()

# Add a Column with Current Date
df_with_date = df.withColumn("current_date", current_date())
df_with_date.show()

# Aggregate by Country with Sum of Phone Numbers
df_agg = df.groupBy("country").agg(sum("phonenumber").alias("total_phone_numbers"))
df_agg.show()

# Join with Another DataFrame
data = [(1, "New York"), (2, "Beirut"), (3, "Cape Town")]
columns = ["id", "city"]
df2 = spark.createDataFrame(data, columns)

df_joined = df.join(df2, on="id", how="inner")
df_joined.show()

# Stop Spark Session
spark.stop()


#to run from the cluster (mobaxterm)
#spark-submit myspark.py
#spark-submit --master local[4] my_script.py

#To run from terminal
#spark-submit --master spark://<master-url>:7077 --deploy-mode client createRDD.py
#spark-submit --master spark://18.132.63.52:7077 --deploy-mode client createRDD.py


#YARN Mode
#Client Mode:

#spark-submit --master yarn --deploy-mode client --num-executors 5 --executor-memory 4G --executor-cores 2 my_script.py

#spark-submit --master yarn --deploy-mode cluster --num-executors 5 --executor-memory 4G --executor-cores 2 my_script.py