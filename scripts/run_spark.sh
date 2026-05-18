#!/bin/bash

echo "START SPARK STREAMING"

docker stop spark-streaming >/dev/null 2>&1 || true
docker rm -f spark-streaming >/dev/null 2>&1 || true

docker run --rm -it \
  --name spark-streaming \
  --hostname spark-client \
  --network=streaming-network \
  -v $(pwd):/spark \
  -v spark_lib:/home/spark/.ivy2 \
  -v spark_data:/data \
  -e HADOOP_CONF_DIR=/spark/hadoop-conf \
  -e PYSPARK_DRIVER_PYTHON=python \
  -e PYSPARK_PYTHON=./environment/bin/python \
  unigap/spark:3.5 bash -c '

    cd /spark &&

    zip -r /tmp/src.zip src >/dev/null &&

    source ~/miniconda3/bin/activate &&

    conda env remove -n pyspark_conda_env -y >/dev/null 2>&1 || true &&

    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main &&

    conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r &&

    conda env create -f environment.yml &&

    conda activate pyspark_conda_env &&

    conda pack --ignore-missing-files -f -o /tmp/pyspark_conda_env.tar.gz &&

    spark-submit \
        --master yarn \
        --deploy-mode client \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
        --conf spark.yarn.dist.archives=/tmp/pyspark_conda_env.tar.gz#environment \
        --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
        --py-files /tmp/src.zip \
        src/main.py
'