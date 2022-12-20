from pyspark.sql import SparkSession

# init spark
spark = SparkSession.builder.appName().getOrCreate()

# load csv to dataframe
df_test = spark.read.option("header",True) \
     .csv("/usr/local/spark/resources/application_test.csv")

df_train = spark.read.option("header",True) \
     .csv("/usr/local/spark/resources/application_train.csv")

# write to mysql
df_test.write.format("jdbc").option("url", "jdbc:mysql://mysql:3306/test") \
	.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "home_credit_default_risk_application_test") \
	.option("user", "root").option("password", "anypassword").save()

df_train.write.format("jdbc").option("url", "jdbc:mysql://mysql:3306/test") \
	.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "home_credit_default_risk_application_train") \
	.option("user", "root").option("password", "anypassword").save()