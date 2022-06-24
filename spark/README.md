This Scala application serves as a Consumer to write Kafka topics into Delta Lake on Spark via spark-submit.

++ Assembly plugin

Requirements:
- sbt 1.6.2 to compile to project and create an uber jar using assembly
- Scala 2.12.15
- Java 11.0.14-librca

To run these Scala applications, an existing Spark Cluster is needed. The most convenient way is to
create a 'Run Configuration' in Intellij using the 'Big Data Tools' plugin. This allows creating a 'spark submit'
command that uploads the uber-jar (after assembly) with all the required dependencies in `build.sbt` to the server running spark, and
then either starts the Application in 'client' or 'cluster' mode. More information can be found
on the Intellij IDEA [website](https://www.jetbrains.com/help/idea/big-data-tools-spark-submit.html).

Example run configurations are available in the ```.run``` directory:
- adjust the SSH configuration for the server to the VM with a key-pair.
- adjust the path to the assembly build jar to be uploaded to the server.

An example command could look like the following:

```
/opt/spark/bin/spark-submit  
    --master spark://HOST:7077 --deploy-mode cluster  
    --class AccountConsumer --name AccountConsumer  
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:1.1.0  
    file:///home/ubuntu/dev/jars/kafka-spark-ingest-assembly-0.1.0-SNAPSHOT.jar
 ```

To build the uber-jar, set the ```spark``` directory as the project root in your console, then run:

```
sbt assembly
```
