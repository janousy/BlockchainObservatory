import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

if __name__ == '__main__':
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '16g'),
        ('spark.executor.cores', '4'),
        ('spark.cores.max', '4'),
        ('spark.driver.memory', '16'),
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

    # Pattern 1

    query1 = """
    MATCH (a1:Account)-[r1:PAYMENT]->(a2:Account)-[r2:PAYMENT]->(a3:Account)
    USING INDEX r1:PAYMENT(amount)
    USING INDEX r2:PAYMENT(amount)
    WHERE r2.amount > 100000000000 AND r1.amount > 100000000000 AND a1.account <> a2.account AND a2.account <> a3.account
    WITH DISTINCT r1 AS rela, a1.account AS senderAccount
    RETURN senderAccount
    """

    print("START PATTERN 1")

    dfPattern1 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query1) \
        .load() \
        .groupBy("senderAccount").count().sort(col("count").desc()) \
        .write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_LargePaymentTransactionAcc_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 1")

    # Pattern 2

    query2 = """
    MATCH (a1:Account)-[r:ASSET_TRANSFER]->(a2:Account)
    USING INDEX r:ASSET_TRANSFER(transferType)
    WHERE a1.account <> a2.account AND r.transferType="transfer"
    WITH a1.account AS senderAccount, count(r) AS rel_count
    RETURN senderAccount, rel_count
    ORDER BY rel_count DESC 
    """

    print("START PATTERN 2")

    dfPattern2 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query2) \
        .load() \
        .write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_ManyAssetTransferAcc_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 2")

    # Pattern 3

    query3 = """
    MATCH (a1:Account)-[r:ASSET_CONFIGURATION]->(a2:Asset)
    WHERE r.configurationType = "creation"
    WITH a1.account AS senderAccount, count(r) AS rel_count
    RETURN senderAccount, rel_count
    ORDER BY rel_count DESC
    """

    print("START PATTERN 3")

    dfPattern3 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query3) \
        .load() \
        .write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_AccAssetCreation_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 3")

    # Pattern 4

    query4 = """
    MATCH (a1:Account)-[r1:APPLICATION_CALL]->(app:Application)<-[r2:APPLICATION_CALL]-(a2:Account)
    USING INDEX r1:APPLICATION_CALL(applicationCallType)
    USING INDEX r2:APPLICATION_CALL(applicationCallType)
    WHERE a1.account <> a2.account AND r1.applicationCallType = "update" AND r2.applicationCallType = "update"
    WITH  DISTINCT app.application AS application, a1.account AS account1, a2.account AS account2
    RETURN DISTINCT application, account1
    ORDER BY application
    """

    print("START PATTERN 4")

    dfPattern4 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query4) \
        .load() \
        .write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_ScUpdatesFromDifferentAcc_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 4")


    # Pattern 5

    query5 = """
    MATCH (a1:Account)-[r:PAYMENT]->(a2:Account) 
    WHERE r.amount < 100000 AND a1.account <> a2.account 
    WITH count(r) AS rel_count, a1.account AS senderAccount, a2.account AS receiverAccount
    WHERE rel_count > 100 
    RETURN senderAccount, receiverAccount, rel_count
    ORDER BY rel_count DESC
    """

    print("START PATTERN 5.1")

    dfPattern5 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query5) \
        .load()

    dfPattern5.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_silver') \
        .option('spark.mongodb.collection', 'Patterns_AccWithManyPaymentTxs_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 5.1")
    print("START PATTERN 5.2")

    dfPattern5Summed = dfPattern5.groupBy("senderAccount").sum("rel_count").sort(col("sum(rel_count)").desc())

    dfPattern5Summed.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_AccWithManyPaymentTxsSum_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 5.2")

    # Graph Projection
    queryGraphProjection = """
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

    print("START PATTERN GRAPH_PROJECTION")

    dfPaymentGraphProjection = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", queryGraphProjection) \
        .option("partitions", "1") \
        .load()

    print("END PATTERN GRAPH_PROJECTION")

    # Pattern 6

    query6 = """
    CALL gds.degree.stream('paymentGraph')
    YIELD nodeId, score
    WITH gds.util.asNode(nodeId).account AS account, score AS degree
    ORDER BY degree DESC LIMIT 50
    RETURN account, degree
    """

    print("START PATTERN 6")

    dfPattern6 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query6) \
        .option("partitions", "1") \
        .load() \
        .write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_DegreeCentrality_Top50_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 6")

    # Pattern 7

    query7 = """
    CALL gds.eigenvector.stream('paymentGraph')
    YIELD nodeId, score
    WITH gds.util.asNode(nodeId).account AS account, score AS eigenVectorScore
    ORDER BY eigenVectorScore DESC LIMIT 10
    RETURN account, eigenVectorScore
    """

    print("START PATTERN 7")

    dfPattern7 = spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", query7) \
        .option("partitions", "1") \
        .load() \
        .write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("overwrite") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', 'Patterns_EigenvectorCentrality_Top10_6') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()

    print("END PATTERN 7")

    # Removing Graph Projection

    queryRemovingGraphProjection = """
    CALL gds.graph.drop('paymentGraph') 
    YIELD graphName 
    RETURN graphName
    """

    print("START PATTERN REMOVE_GRAPH_PROJECTION")

    spark.read.format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://172.23.149.212:7687") \
        .option("query", queryRemovingGraphProjection) \
        .load()

    print("END PATTERN REMOVE_GRAPH_PROJECTION")

    # Stopping spark context
    spark.stop()
    raise KeyboardInterrupt
