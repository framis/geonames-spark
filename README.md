Spark Geonames Importer
=======================

This Spark Job downloads all cities from the [Geoname database](http://www.geonames.org)
It merges the countries, administrative information and postal codes to the cities.
The end result is a DataFrame that contains all the cities for further processing.

To download the geonames files, run :

```
./download.sh
```

To start Spark and run the job

```
docker-compose run spark                                   # To start the spark container

# And from within the spark container
spark-shell                                                # To start spark-shell from within the container
spark-shell --packages "com.algolia:scala-client_2.11:1.0.1-SNAPSHOT" \
--jars "/app/scala-client_2.11-1.0.1-SNAPSHOT.jar" \
--repositories https://oss.sonatype.org/content/repositories/snapshots/
--conf spark.es.nodes="es" \
 spark.es.nodes.wan.only="true" \
 spark.es.index.auto.create="true"
sbt assembly                                               # Rebuilds the JAR  
spark-submit target/scala-2.11/geoname-assembly-1.0.jar    # Runs the job in local mode
```