
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

#%% Import libraries

