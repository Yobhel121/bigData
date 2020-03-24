/opt/modules/spark-1.5.1-bin-hadoop2.4/bin/spark-submit \
--class cn.spark.study.sql.DataFrameCreate \
--num-executors 3 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 3 \
--files /opt/modules/hive-0.13.1-bin/conf/hive-site.xml \
--driver-class-path /opt/modules/hive-0.13.1-bin/lib/mysql-connector-java-5.1.17.jar \
/opt/data/spark-study/java/sql/spark-test-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
