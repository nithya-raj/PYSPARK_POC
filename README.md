In order to able to run a Pyspark script on the EMR cluster and write into Redshift, the following dependent jar files need to be set up
---------------------------------------------------------------------------------------------------------------------------------------

spark-redshift_2.10-2.0.0.jar
minimal-json-0.9.5.jar
spark-avro_2.11-3.0.0.jar
aws-java-sdk-core-1.11.118.jar
aws-java-sdk-redshift-1.11.118.jar
aws-java-sdk-sts-1.11.118.jar
jackson-dataformat-cbor-2.6.6.jar
redshift-jdbc42-2.1.0.5.jar


The jar files can be obtained from the following links
------------------------------------------------------


wget https://repo1.maven.org/maven2/com/databricks/spark-redshift_2.10/2.0.0/spark-redshift_2.10-2.0.0.jar

wget https://github.com/ralfstx/minimal-json/releases/download/0.9.5/minimal-json-0.9.5.jar

wget https://repo1.maven.org/maven2/com/databricks/spark-avro_2.11/3.0.0/spark-avro_2.11-3.0.0.jar


wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.118/aws-java-sdk-core-1.11.118.jar

wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-redshift/1.11.118/aws-java-sdk-redshift-1.11.118.jar

wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-sts/1.11.118/aws-java-sdk-sts-1.11.118.jar

wget https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.6.6/jackson-dataformat-cbor-2.6.6.jar

wget https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.5/redshift-jdbc42-2.1.0.5.jar


Command to invoke pyspark
-----------------------------


pyspark --jars spark-redshift_2.10-2.0.0.jar,redshift-jdbc42-2.1.0.5.jar,minimal-json-0.9.5.jar,spark-avro_2.11-3.0.0.jar
