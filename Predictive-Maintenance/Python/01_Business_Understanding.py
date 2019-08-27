
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
import plotly
plotly.__version__
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
init_notebook_mode(connected=True)

#%% Import libraries

sc = pyspark.SparkContext(appName="Business_Understanding")
spark = SparkSession(sc)

#%% Read data

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# to read parquet file
telemetry = sqlContext.read.parquet("../Python/input/PdM_telemetry.parquet")
errors = sqlContext.read.parquet("../Python/input/PdM_errors.parquet")
maint = sqlContext.read.parquet("../Python/input/PdM_maint.parquet")
failures = sqlContext.read.parquet("../Python/input/PdM_failures.parquet")
machines = sqlContext.read.parquet("../Python/input/PdM_machines.parquet")

#%% [markdown] 
# ## Telemetry

# The first data source is the telemetry time-series data which consists of voltage, rotation, pressure, and vibration measurements collected from 100 machines in real time averaged over every hour collected during the year 2015. Below, we display the first 10 records in the dataset. A summary of the whole dataset is also provided.


def telemetry_plot(machine, date_from, date_to):
    """Append features from columns to the features vector.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
    cols : list of str

    Returns
    -------
    pyspark.sql.DataFrame
    """
    
    plot_df = telemetry.filter(telemetry.machineID == machine).where((telemetry.datetime > date_from) & (telemetry.datetime < date_to)).select(telemetry.datetime, telemetry.volt)
    data = [go.Scatter(x = plot_df.toPandas().datetime, y=plot_df.toPandas()['volt'])]
    return iplot(data)

#%% Plot Telemetry

telemetry_plot(machine = 1, date_from = '2015-01-01', date_to = '2015-02-01')

#%% [markdown] 
# ## Errors

# The second major data source is the error logs. These are non-breaking errors thrown while the machine is still operational and do not constitute as failures. The error date and times are rounded to the closest hour since the telemetry data is collected at an hourly rate.

errors.createOrReplaceTempView("errors")
data = spark.sql("""SELECT COUNT(datetime) as Total_Error, errorID 
                 FROM errors
                 GROUP BY errorID""")

trace = [go.Pie(labels = data.toPandas().errorID, values= data.toPandas().Total_Error)]
iplot(trace)

#%% [markdown] 
# ## Maintenance

# These are the scheduled and unscheduled maintenance records which correspond to both regular inspection of components as well as failures. A record is generated if a component is replaced during the scheduled inspection or replaced due to a breakdown. The records that are created due to breakdowns will be called failures which is explained in the later sections. Maintenance data has both 2014 and 2015 records.

maint.createOrReplaceTempView("maint")
data = spark.sql("""SELECT COUNT(datetime) as Total_Error, comp 
                 FROM maint
                 GROUP BY comp""")

trace = [go.Pie(labels = data.toPandas().comp, values= data.toPandas().Total_Error)]
iplot(trace)

#%% [markdown] 
# ## Machines

# This data set includes some information about the machines: model type and age (years in service).

%matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns


machines_pandas = machines.toPandas()
machines_pandas['model'] = machines_pandas['model'].astype('category')
sns.set_style("darkgrid")
plt.figure(figsize=(15, 6))
_, bins, _ = plt.hist([machines_pandas.loc[machines_pandas['model'] == 'model1', 'age'],
                       machines_pandas.loc[machines_pandas['model'] == 'model2', 'age'],
                       machines_pandas.loc[machines_pandas['model'] == 'model3', 'age'],
                       machines_pandas.loc[machines_pandas['model'] == 'model4', 'age']],
                       20, stacked=True, label=['model1', 'model2', 'model3', 'model4'])
plt.xlabel('Age (yrs)')
plt.ylabel('Count')
plt.legend()


#%% [markdown] 
# ## Failures

# These are the records of component replacements due to failures. Each record has a date and time, machine ID, and failed component type.

failures.createOrReplaceTempView("failures")
data = spark.sql("""SELECT COUNT(datetime) as Total_Error, failure 
                 FROM failures
                 GROUP BY failure""")

trace = [go.Pie(labels = data.toPandas().failure, values= data.toPandas().Total_Error)]
iplot(trace)

#%%
