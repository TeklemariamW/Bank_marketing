## 1. Incremental Load Process
The incremental load process involves determining and processing only the new data added since the last load. This process includes:

1. Importing necessary modules.
2. Creating a Spark session with Hive support.
3. Establishing connection to PostgreSQL and reading data.
4. Performing data transformations.
5. Reading existing data from the Hive table.
6. Determining the incremental data.
7. Counting the number of new records added to PostgreSQL.
8. Adding the incremental data to the existing Hive table.
9. Stopping Spark session.

## 2. pyspark incremental load code Overview:
```
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper

# Create spark session with hive enabled
spark = SparkSession.builder \
        .appName("BankMarketing") \
        .enableHiveSupport() \
        .getOrCreate()

# 1- Establish the connection to PostgresSQL and hive:

# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

try:
    postgres_table_name = "bank"

    # read data from postgres table into dataframe :
    df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
    df_postgres.printSchema()
    df_postgres.show(3)

    ## 2. load df_postgres to hive table
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS tekle")

    # Hive database and table names
    hive_database_name = "tekle"
    hive_table_name = "bank_marketing"

    # change job column into upper case
    df_upper = df_postgres.withColumn("job_upper", upper(df_postgres['job']))
    df_upper.show(5)

    # read and show the existing_data in hive table
    existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))

    existing_hive_data.show(3)

    # 4. Determine the incremental data
    '''
    "left_anti" specifies the type of join to perform. 
    A left_anti join returns only the rows from the left DataFrame (df_upper in this case) 
    that do not have corresponding matches in the right DataFrame (existing_hive_data). 
    Essentially, it finds rows in df_upper that are not present in existing_hive_data based 
    on id
    '''
    incremental_data_df = df_upper.join(existing_hive_data.select("id"), df_upper["id"] == existing_hive_data["id"], "left_anti")
    print('------------------Incremental data-----------------------')
    incremental_data_df.show()

    # counting the number of the new records added to postgres tables
    new_records = incremental_data_df.count()
    print('------------------COUNTING INCREMENT RECORDS ------------')
    print('new records added count', new_records)

    # 5.  Adding the incremental_data DataFrame to the existing hive table
    # Check if there are extra rows in PostgresSQL. if exist => # write & append to the Hive table
    if incremental_data_df.count() > 0:
        # Append new rows to Hive table
        incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
        #incremental_data_df.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))
        print("Appended {} new records to Hive table.".format(incremental_data_df.count()))
    else:
        print("No new records been inserted in PostgresSQL table.")

    # Read again from hive to see the updated data
    updated_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
    df_ordered = updated_hive_data.orderBy(updated_hive_data["id"].desc())
    df_ordered.show()
except Exception as ex:
    print("Error reading data from PostgreSQL or saving to Hive:", ex)

finally:
    spark.stop()


# spark-submit --jars postgresql-42.6.0.jar pyspark_incrementalLoad.py

```

## 3. The provided PySpark code executes the following steps:

### Step 1: Importing Necessary Modules
The code imports essential modules from the PySpark library required for data processing.

### Step 2: Creating Spark Session with Hive Support
A Spark session is created with Hive support enabled, allowing interaction with Hive databases.

### Step 3: Establishing Connection to PostgreSQL and Reading Data
Connection properties for PostgreSQL are defined, and data is read from the PostgreSQL table into a DataFrame.

### Step 4: Determining Incremental Data
The code determines the incremental data by performing a left anti-join between the data from PostgreSQL and the existing data in the Hive table, based on the "POLICY_NUMBER" column.

### Step 5: Counting New Records Added to PostgreSQL Tables
The code counts the number of new records added to the PostgreSQL table, which will be appended to the Hive table.

### Step 6: Adding Incremental Data to Existing Hive Table
If there are new records, they are appended to the existing Hive table. Otherwise, a message indicating no new records have been inserted is displayed.

### Step 7: Stopping Spark Session
The Spark session is stopped to release resources once all data processing tasks are completed.

## 4. GitHub and Jenkins Integration

GitHub serves as the repository for hosting the PySpark code. It allows version control, collaboration, and code sharing.
Jenkins is utilized for Continuous Integration/Continuous Deployment (CI/CD) processes. It monitors GitHub repositories for changes and triggers automated builds and tests whenever new code is pushed.
Jenkins executes the PySpark job defined in the code whenever there's a new commit or code change in the GitHub repository.

link to github  repo : https://github.com/TeklemariamW/Bank_marketing/blob/master/CICD/src/pyspark_incrementalLoad.py 

link to pyspark incremental load job  on jenkins : http://3.9.191.104:8080/view/Tekle/job/TestPysparkBanking/


## 5. Explanation
The code reads data from a PostgreSQL database, determines incremental data, and appends it to an existing Hive table.
GitHub ensures version control and collaborative development of the PySpark code.
Jenkins automates the execution of the PySpark job as part of the CI/CD pipeline, ensuring seamless deployment and data processing.

## 6. Conclusion
This documentation outlines the process of incremental data processing using PySpark, emphasizing the integration of GitHub and Jenkins for code hosting and CI/CD orchestration. The provided PySpark code efficiently handles incremental data updates, ensuring data integrity and reliability in the data processing pipeline.