# init docker
## create docker network
``
docker network create default_network
``

# airflow
``
docker-compose up -d --build
``

# spark
``
docker-compose up -d
spark-submit --jars /usr/local/spark/resources/mysql-connector-java-8.0.30.jar --name csv_to_mysql /usr/local/spark/app/csv_to_mysql.py
spark-submit --jars /usr/local/spark/resources/mysql-connector-java-8.0.30.jar,/usr/local/spark/resources/postgresql-42.5.1.jar --name mysql_to_postgres /usr/local/spark/app/mysql_to_postgres.py
``

# kafka
``
docker-compose up -d
``

# postgresql
``
docker run --name postgres-ds9 --network=default_network --hostname postgres -e POSTGRES_PASSWORD=anypassword -p 5432:5432 -d postgres
``

# mysql
``
docker run --name mysql-ds9 --network=default_network --hostname mysql -e MYSQL_ROOT_PASSWORD=anypassword -p 3306:3306 -d mysql
``