from pyspark import SparkConf, SparkContext

# SparkConf : Configures spark parameters
# local: For running spark on local machine (cluster address can be specified)
# My app: Name which will be visible on cluster manager's UI
conf = SparkConf().setMaster("local").setAppName("My app")

# SparkContext: Hides connection to the cluster. sc variable can be used throught application 
#               for computational work.
# conf: Create spark context with previously specified configurations
sc = SparkContext(conf=conf)

# Start using spark context from here
print(sc)

# stop spark context (Optional)
sc.stop()
