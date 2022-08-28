from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"D:\\bigdata\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
data=conf.get("input","data")
#tabs=['dept','emp','loan','adipr','empclean']
qry="(select table_name from information_schema.TABLES where TABLE_SCHEMA='mysqldatabase')aaa"
df1=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",qry).option("driver","com.mysql.jdbc.Driver").load()
tabs=[x[0] for x in df1.collect() if df1.count()>0]
for i in tabs:
    df=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("dbtable",i).option("driver","com.mysql.jdbc.Driver").load()
    df.show()
