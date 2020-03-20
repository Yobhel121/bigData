/opt/modules/spark-1.5.1-bin-hadoop2.4/bin/spark-submit \
--master local[2] \
--class cn.spark.study.core.WordCountCluster \
--num-executors 3 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 3 \
/opt/data/spark-study/spark-study-java-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
