from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
host="jdbc:mysql://mysqldb.cw5nrr6tfuay.ap-south-1.rds.amazonaws.com:3306/mysqldatabase?useSSL=false"
qry="(select * from empclean where comm>300) t" # t is temporary variable
df=spark.read.format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
    .option("dbtable",qry).option("driver","com.mysql.jdbc.Driver").load()
res=df
#df.show()
#res=df.na.fill(0,['comm','mgr']).withColumn("comm", col("comm").cast(IntegerType()))\
 #   .withColumn("hiredate", date_format(col("hiredate"),"yyyy/MMM/dd"))
#res.write.mode("overwrite").format("jdbc").option("url",host).option("user","myuser").option("password","mypassword")\
 #   .option("dbtable","emp").option("driver","com.mysql.jdbc.Driver").save()
res.show()





