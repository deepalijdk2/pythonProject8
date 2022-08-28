from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="D:\\bigdata\\datasets\\IBM.csv"
df=spark.read.format("csv").option("header","true").load(data)
df.show()
odata="D:\\bigdata\\datasets\\INFY.csv"
ndf=spark.read.format("csv").option("header","true").load(odata)
ndf.show()
ds3=data.union(odata)

#df=spark.read.format("csv").load(data)
#rdd=spark.sparkContext.textFile(data)
#skip=rdd.first()
#odata=rdd.filter(lambda x:x!=skip)
#df=spark.read.csv(odata,header=True)
#df.printSchema()
#df.show()