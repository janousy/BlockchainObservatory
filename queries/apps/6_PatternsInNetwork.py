import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '16g'),
        ('spark.executor.cores', '4'),
        ('spark.cores.max', '4'),
        ('spark.driver.memory', '64g'),
        ('spark.executor.instances', '1'),
        ('spark.worker.cleanup.enabled', 'true'),
        ('spark.worker.cleanup.interval', '60'),
        ('spark.worker.cleanup.appDataTtl', '60'),
        ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
    ])

    spark = SparkSession \
        .builder \
        .config(conf=config) \
        .appName("6_PatternsInNetworkApplication") \
        .master("spark://172.23.149.212:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("DEBUG")

    dfPattern1 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query",
                "MATCH (a1:Account)-[r1:PAYMENT]->(a2:Account)-[r2:PAYMENT]->(a3:Account) WHERE a1.account <> a2.account AND r1.amount > 100000000 AND r2.amount > 100000000 AND r1.blockNumber > 0 AND r2.blockNumber > 0 AND r2.blockNumber - r1.blockNumber < 17280 RETURN a1.account AS senderAccount") \
        .load()

    spark.stop()
    raise KeyboardInterrupt
