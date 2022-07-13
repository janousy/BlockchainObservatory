SPARK_MASTER_HOST=172.23.149.212
SPARK_MASTER_PORT=7077
SPARK_MASTER_WEBUI_PORT=28080
SPARK_WORKER_WEBUI_PORT=28081
SPARK_WORKERS_OPTS="spark.worker.cleanup.enabled=true"

SPARK_WORKER_OPTS="$SPARK_WORKER_OPTS -Dspark.worker.cleanup.enabled=true
        -Dspark.executor.logs.rolling.strategy=time \
        -Dspark.executor.logs.rolling.time.interval=hourly \
        -Dspark.executor.logs.rolling.maxRetainedFiles=3"