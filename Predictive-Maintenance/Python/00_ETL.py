#%% [markdown] 
# #ETL

# ## Data Sources

# * **Failure history**: The failure history of a machine or component within the machine.
# * **Maintenance history**: The repair history of a machine, e.g. error codes, previous maintenance activities or component replacements.
# * **Machine conditions and usage**: The operating conditions of a machine e.g. data collected from sensors.
# * **Machine features**: The features of a machine, e.g. engine size, make and model, location.
# * **Operator features**: The features of the operator, e.g. gender, past experience The data for this example comes from 4 different sources which are real-time telemetry data collected from machines, error messages, historical maintenance records that include failures and machine information such as type and age.


#%% Import libraries
import findspark
findspark.init()

findspark.find()
import pyspark
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sc = pyspark.SparkContext(appName="ETL")
spark = SparkSession(sc)

#%% Defining Schemas
    
telemetry_schema = StructType([
  # Define a StructField for each field
  StructField('datetime', TimestampType(), False),
  StructField('machineID', IntegerType(), False),
  StructField('volt', DoubleType(), False),
  StructField('rotate', DoubleType(), False),
  StructField('pressure', DoubleType(), False),
  StructField('vibration', DoubleType(), False)
])

errors_schema = StructType([
  # Define a StructField for each field
  StructField('datetime', TimestampType(), False),
  StructField('machineID', IntegerType(), False),
  StructField('errorID', StringType(), False)
])

maint_schema = StructType([
  # Define a StructField for each field
  StructField('datetime', TimestampType(), False),
  StructField('machineID', IntegerType(), False),
  StructField('comp', StringType(), False)
])

failures_schema = StructType([
  # Define a StructField for each field
  StructField('datetime', StringType(), False),
  StructField('machineID', IntegerType(), False),
  StructField('failure', StringType(), False)
])

machines_schema = StructType([
  # Define a StructField for each field
  StructField('machineID', IntegerType(), False),
  StructField('model', StringType(), False),
  StructField('age', IntegerType(), False)
])

#%% Reading logs
telemetry = spark.read.csv('../data/PdM_telemetry.csv', header = True, schema = telemetry_schema)
errors = spark.read.csv('../data/PdM_errors.csv', header = True, schema = errors_schema)
maint = spark.read.csv('../data/PdM_maint.csv', header = True, schema = maint_schema)
failures = spark.read.csv('../data/PdM_failures.csv', header = True, schema = failures_schema)
machines = spark.read.csv('../data/PdM_machines.csv', header = True, schema = machines_schema)

#%% Writing data in parquet format
telemetry.write.parquet("../Python/input/PdM_telemetry.parquet", mode='overwrite')
errors.write.parquet("../Python/input/PdM_errors.parquet",mode='overwrite')
maint.write.parquet("../Python/input/PdM_maint.parquet",mode='overwrite')
failures.write.parquet("../Python/input/PdM_failures.parquet",mode='overwrite')
machines.write.parquet("../Python/input/PdM_machines.parquet",mode='overwrite')

#%%
