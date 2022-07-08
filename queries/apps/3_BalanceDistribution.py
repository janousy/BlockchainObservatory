import matplotlib.pyplot as plt
import numpy as np

import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, hex, base64, avg, collect_list, concat, lit, mean
import pyspark.sql.functions as F

if __name__ == '__main__':
    config = pyspark.SparkConf().setAll([
        ('spark.executor.memory', '16g'),
        ('spark.executor.cores', '4'),
        ('spark.cores.max', '8'),
        ('spark.driver.memory', '2g'),
        ('spark.executor.instances', '1'),
        ('spark.dynamicAllocation.enabled', 'true'),
        ('spark.dynamicAllocation.shuffleTracking.enabled', 'true'),
        ('spark.dynamicAllocation.executorIdleTimeout', '60s'),
        ('spark.dynamicAllocation.minExecutors', '2'),
        ('spark.dynamicAllocation.maxExecutors', '2'),
        ('spark.dynamicAllocation.initialExecutors', '1'),
        ('spark.dynamicAllocation.executorAllocationRatio', '1'),
        ('spark.worker.cleanup.enabled', 'true'),
        ('spark.worker.cleanup.interval', '60'),
        ('spark.shuffle.service.db.enabled', 'true'),
        ('spark.worker.cleanup.appDataTtl', '60'),
        ('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector:10.0.2')
    ])

    # create sparksession
    # when copying change appName
    spark = SparkSession \
        .builder \
        .config(conf=config) \
        .appName("balanceDistrAPP") \
        .master("spark://172.23.149.212:7077") \
        .getOrCreate()

    # account table to determine which accounts have the highest balances
    dfAccounts = spark.read.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .option('spark.mongodb.database', 'algorand') \
        .option('spark.mongodb.collection', 'account') \
        .option('park.mongodb.read.readPreference.name', 'primaryPreferred') \
        .option('spark.mongodb.change.stream.publish.full.document.only', 'true') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .load()


    # organizing table
    # drop all unneccessary columns
    dfAccounts = dfAccounts.drop("_id", "rewardsbase", "account_data", "rewards_total", "deleted", "closed_at",
                                 "keytype")

    # calculate hom many algos are on the chain, and print the amount of algos on the chain
    totalAlgos = dfAccounts.agg(F.sum("microalgos")).collect()[0][0]

    # add column and calculate the proportion of the account to all algos, proportion is in %
    dfAccounts = dfAccounts.withColumn("proportion", dfAccounts.microalgos / totalAlgos * 100)

    totalAccounts = dfAccounts.count()


    # write the results into a gold table
    newestRound = dfAccounts.agg(F.max("created_at")).collect()[0][0]

    result = spark.createDataFrame(
        [
            (totalAlgos, totalAlgos / 1000, totalAccounts, newestRound)
            # create your data here, be consistent in the types.

        ],
        ["totalMicroAlgos", "totalAlgos", "totalAccounts", "CreationRound"]  # add your column names here
    )

    result.write.format("mongodb") \
        .option('spark.mongodb.connection.uri', 'mongodb://172.23.149.212:27017') \
        .mode("append") \
        .option('spark.mongodb.database', 'algorand_gold') \
        .option('spark.mongodb.collection', '3_TotalAlgos') \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .save()


    # Plotting balance distribution
    # everything with 0
    dataWith0Accounts = dfAccounts.select("microalgos")
    dataWith0Accounts = dataWith0Accounts.collect()

    # convert row["data"] to only data
    microalgos0 = [row[0] for (row) in dataWith0Accounts]
    mean_alg0 = dfAccounts.agg(F.mean("microalgos")).collect()[0][0]

    # histogram with all accounts
    # histogram
    bin_size = 100
    mybins = np.logspace(0, np.log10(max(microalgos0)), bin_size)

    mybins = np.insert(mybins, 0, 0)

    plt.hist(microalgos0, bins=mybins)
    # plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("microalgos")
    plt.ylabel("accounts")
    plt.title("Distribution of Account Balances", loc='center', pad=None)
    plt.axvline(mean_alg0, color='k', linestyle='dashed', linewidth=1)
    plt.savefig('/home/ubuntu/apps/figures/3_BalanceDistribution/Distribution_AccountBalances_incl_0.jpg', dpi=200)

    # cell with no 0 values
    # get rid off 0 values because they are destroying the plot
    dfAccNoZero = dfAccounts.filter(dfAccounts.microalgos > 0)

    # graph
    dataWithout0Accounts = dfAccNoZero.select("microalgos")
    dataWithout0Accounts = dataWithout0Accounts.collect()

    # convert row["data"] to only data
    microalgos = [row[0] for (row) in dataWithout0Accounts]

    # calculate the mean of all accounts with a balance > 0
    mean_alg = dfAccNoZero.agg(F.mean("microalgos")).collect()[0][0]

    # histogram with all accounts with an amount > 0
    # histogram
    bin_size = 100
    # distribute bins log(equally) over the whole data
    mybins = np.logspace(np.log10(min(microalgos)), np.log10(max(microalgos)), bin_size)
    plt.hist(microalgos, bins=mybins)
    # plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel("microalgos")
    plt.ylabel("accounts")
    plt.title("Distribution of Account Balances >0 ", loc='center', pad=None)
    plt.axvline(mean_alg, color='k', linestyle='dashed', linewidth=1)
    plt.savefig('/home/ubuntu/apps/figures/3_BalanceDistribution/Distribution_AccountBalances_excl_0.jpg', dpi=200)

    # graph select only account balances, sort it from highest to lowest and take the highest 10 balances
    whalesData = dfAccounts.select("microalgos", "addr").sort(col("microalgos").desc()).head(10)

    # convert row["data"] to only data /1000 to reach algos from microalgos
    whales = [row[0] / 1000 for (row) in whalesData]
    whalesAddresses = [row[1] for (row) in whalesData]

    # save the whales, the top 10 whales are saved in a list
    plt.bar("whale1", whales[0], width=0.4)
    plt.bar("whale2", whales[1], width=0.4)
    plt.bar("whale3", whales[2], width=0.4)
    plt.bar("whale4", whales[3], width=0.4)
    plt.bar("whale5", whales[4], width=0.4)
    plt.rcParams["figure.figsize"] = (10, 5)
    plt.title("Biggest whales with their account balances in Algos", loc='center', pad=None)
    plt.legend([whalesAddresses[0], whalesAddresses[1], whalesAddresses[2], whalesAddresses[3], whalesAddresses[4]])
    plt.savefig('/home/ubuntu/apps/figures/3_BalanceDistribution/Distribution_whales.jpg', dpi=200)

    spark.stop()
