This Scala application serves as a Consumer to write Kafka topics into Delta Lake on Spark via spark-submit.

Requirements:
- sbt 1.6.2 to compile to project and create an uber jar using assembly
- Scala 2.12.15
- Java 11.0.14-librca

To run this application, an existing Spark Cluster is needed. The most convenient way is to
create a 'Run Configuration' in Intellij using the 'Big Data Tools' plugin. This allows creating a 'spark submit'
command that uploads the uber-jar with all the required dependencies in `build.sbt` to the server running spark, and
then either starts the Application in 'client' or 'cluster' mode.

An example command could look like the following:

```
/opt/spark/bin/spark-submit  
    --master spark://HOST:7077 --deploy-mode cluster  
    --class AccountsConsumer --name AccountsConsumer  
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:1.1.0  
    file:///home/ubuntu/dev/jars/kafka-spark-ingest-assembly-0.1.0-SNAPSHOT.jar
 ```

