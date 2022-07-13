import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '48g'),
        ('spark.executor.cores', '4'),
        ('spark.cores.max', '4'),
        ('spark.driver.memory', '48g'),
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

    # Pattern 2

    query = """
    MATCH (a1:Account)-[r1:APPLICATION_CALL]->(app:Application)<-[r2:APPLICATION_CALL]-(a2:Account) 
    WHERE a1.account <> a2.account AND r1.blockNumber > 0 AND r2.blockNumber > 0 AND abs(r2.blockNumber - r1.blockNumber) < 17280 
    WITH a1.account AS account, app.application AS application, r1.blockNumber as blockNumber
    RETURN DISTINCT application, account, blockNumber
    """

    dfPattern3 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query) \
        .load()

    dfPattern3.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_ScCallsFromDifferentAcc_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # Pattern 3

    query = """
    MATCH (a1:Account)-[r:PAYMENT]->(a2:Account) 
    WHERE r.amount < 100000 AND a1.account <> a2.account 
    WITH count(r) AS rel_count, a1.account AS senderAccount, a2.account AS receiverAccount
    WHERE rel_count > 100 
    RETURN senderAccount, receiverAccount, rel_count
    ORDER BY rel_count DESC
    """

    dfPattern4 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query) \
        .load()

    dfPattern4.show()

    dfPattern4.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_AccountsWithManyPaymentTransactions_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # Graph Projection
    query = """
     CALL gds.graph.project(
      "paymentGraph",
      "Account",                         
      {
        PAYMENT: {properties: ["blockNumber", "amount"]}
      }           
    )
     YIELD
      graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipProjection, relationshipCount AS rels
     RETURN graph, nodeProjection, nodes, relationshipProjection, rels
    """

    dfPaymentGraphProjection = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query) \
        .option("partitions", "1") \
        .load()

    # Pattern 4

    query = """
    CALL gds.degree.stream('paymentGraph')
    YIELD nodeId, score
    WITH gds.util.asNode(nodeId).account AS account, score AS degree
    ORDER BY degree DESC limit 50
    RETURN account, degree
    """

    dfPattern5 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query) \
        .option("partitions", "1") \
        .load()

    dfPattern5.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_DegreeCentrality_Top50_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # Pattern 5

    query = """
    CALL gds.eigenvector.stream('paymentGraph')
    YIELD nodeId, score
    WITH gds.util.asNode(nodeId).account AS account, score as eigenVectorScore
    ORDER BY eigenVectorScore DESC limit 10
    RETURN account, eigenVectorScore
    """

    dfPattern6 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query) \
        .option("partitions", "1") \
        .load()

    dfPattern6.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_EigenvectorCentrality_Top10_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    # Removing Graph Projection

    query = """
    CALL gds.graph.drop('paymentGraph') 
    YIELD graphName 
    RETURN graphName
    """

    spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query) \
        .load()

    # Stopping spark context

    spark.stop()
    raise KeyboardInterrupt
