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
sbt assembly                                               # Rebuilds the JAR  
spark-submit target/scala-2.11/geoname-assembly-1.0.jar    # Runs the job in local mode
```