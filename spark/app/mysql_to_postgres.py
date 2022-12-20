from pyspark.sql import SparkSession

# init spark
spark = SparkSession.builder.appName().getOrCreate()

# read mysql
df_test = spark.read.format("jdbc").option("url", "jdbc:mysql://mysql:3306/test") \
	.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "home_credit_default_risk_application_test") \
	.option("user", "root").option("password", "anypassword").load()

df_train = spark.read.format("jdbc").option("url", "jdbc:mysql://mysql:3306/test") \
	.option("driver", "com.mysql.jdbc.Driver").option("dbtable", "home_credit_default_risk_application_train") \
	.option("user", "root").option("password", "anypassword").load()

# write to postgres
df_test.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/test") \
	.option("driver", "org.postgresql.Driver").option("dbtable", "home_credit_default_risk_application_test") \
	.option("user", "root").option("password", "anypassword").save()

df_train.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/test") \
	.option("driver", "org.postgresql.Driver").option("dbtable", "home_credit_default_risk_application_train") \
	.option("user", "root").option("password", "anypassword").save()