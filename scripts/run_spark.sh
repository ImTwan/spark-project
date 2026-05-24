echo "=== Submit Spark job ==="

docker container stop spark-streaming 2>/dev/null || true
docker container rm spark-streaming 2>/dev/null || true

docker run --rm -it \
  --name spark-streaming \
  --hostname spark-client \
  --network=streaming-network \
  -v ./:/spark \
  -v spark_lib:/home/spark/.ivy2 \
  -v spark_data:/data \
  -e HADOOP_CONF_DIR=/spark/infrastructure/hadoop/00-setup/hadoop/conf \
  unigap/spark:3.5 bash -c "

    cd /spark &&

    conda env remove -n pyspark_conda_env -y >/dev/null 2>&1 || true &&

    conda env create -f environment.yml &&

    source ~/miniconda3/bin/activate &&

    conda activate pyspark_conda_env &&

    pip install conda-pack &&

    conda pack -n pyspark_conda_env -o /tmp/pyspark_env.tar.gz &&

    rm -f /tmp/src.zip && zip -r /tmp/src.zip src &&

    spark-submit \
      --master yarn \
      --deploy-mode client \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --files /spark/data/ip2location/IP-COUNTRY-REGION-CITY.BIN#IP-COUNTRY-REGION-CITY.BIN \
      --archives /tmp/pyspark_env.tar.gz#environment \
      --conf spark.yarn.stagingDir=hdfs:///user/spark \
      --conf spark.pyspark.python=./environment/bin/python \
      --conf spark.pyspark.driver.python=/home/spark/miniconda3/envs/pyspark_conda_env/bin/python \
      --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true \
      --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
      --py-files /tmp/src.zip \
      /spark/src/main.py
"

