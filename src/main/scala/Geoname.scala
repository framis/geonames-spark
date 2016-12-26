import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType, DoubleType, FloatType}

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Geoname")
    val sc = new SparkContext(conf)

    val CITY_CODE = "P"
    val GEONAME_PATH = "download/allCountries.txt"
    val COUNTRY_PATH = "download/countryInfo.txt"
    val ADMIN_ONE_PATH = "download/admin1CodesASCII.txt"
    val ADMIN_TWO_PATH = "download/admin2Codes.txt"
    val POSTAL_CODE_PATH = "download/zip/allCountries.txt"

    val geonameSchema = StructType(Array(
        StructField("geonameid", IntegerType, false),
        StructField("name", StringType, false),
        StructField("asciiname", StringType, true),
        StructField("alternatenames", StringType, true),
        StructField("latitude", FloatType, true),
        StructField("longitude", FloatType, true),
        StructField("fclass", StringType, true),
        StructField("fcode", StringType, true),
        StructField("country", StringType, true),
        StructField("cc2", StringType, true),
        StructField("admin1", StringType, true),
        StructField("admin2", StringType, true),
        StructField("admin3", StringType, true),
        StructField("admin4", StringType, true),
        StructField("population", DoubleType, true), // Asia population overflows Integer
        StructField("elevation", IntegerType, true),
        StructField("gtopo30", IntegerType, true),
        StructField("timezone", StringType, true),
        StructField("moddate", DateType, true)))

    val adminSchema = StructType(Array(
        StructField("code", StringType, true),
        StructField("name", StringType, true)))

    val countrySchema = StructType(Array(
        StructField("iso_alpha2", StringType, false),
        StructField("iso_alpha3", StringType, true),
        StructField("iso_numeric", StringType, true),
        StructField("fips_code", StringType, true),
        StructField("name", StringType, true),
        StructField("capital", StringType, true),
        StructField("areainsqkm", StringType, true),
        StructField("population", StringType, true),
        StructField("continent", StringType, true),
        StructField("tld", StringType, true),
        StructField("currency", StringType, true),
        StructField("currencyName", StringType, true),
        StructField("phone", StringType, true),
        StructField("postalCodeFormat", StringType, true),
        StructField("postalCodeRegex", StringType, true),
        StructField("geonameId", StringType, true),
        StructField("languages", StringType, true),
        StructField("neighbours", StringType, true),
        StructField("equivalentFipsCode", StringType, true)))

    val postalCodeSchema = StructType(Array(
        StructField("country", StringType, false),
        StructField("postal_code", StringType, true),
        StructField("name", StringType, true),
        StructField("admin1_name", StringType, true),
        StructField("admin1_code", StringType, true),
        StructField("admin2_name", StringType, true),
        StructField("admin2_code", StringType, true),
        StructField("admin3_name", StringType, true),
        StructField("admin3_code", StringType, true),
        StructField("latitude", StringType, true),
        StructField("longitude", StringType, true),
        StructField("accuracy", IntegerType, true)))

    val getAdminOneCode = udf( (country: String, adminOne: String) => { country + "." + adminOne } )
    val getAdminTwoCode = udf( (country: String, adminOne: String, adminTwo: String) => { country + "." + adminOne + "." + adminTwo } )

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val geonames = sqlContext.read
        .option("header", false)
        .option("quote", "")
        .option("delimiter", "\t")
        .option("maxColumns", 22)
        .schema(geonameSchema)
        .csv(GEONAME_PATH)
        .filter($"fclass"===CITY_CODE)
        .filter($"population">0)
        .withColumn("adminOneCode", getAdminOneCode($"country", $"admin1"))
        .withColumn("adminTwoCode", getAdminTwoCode($"country", $"admin1", $"admin2"))

    val countries = sqlContext.read
        .option("header", false)
        .option("delimiter", "\t")
        .schema(countrySchema)
        .csv(COUNTRY_PATH)

    val adminOne = sqlContext.read
        .option("header", "false")
        .option("delimiter", "\t")
        .schema(adminSchema)
        .csv(ADMIN_ONE_PATH)

    val adminTwo = sqlContext.read
        .option("header", "false")
        .option("delimiter", "\t")
        .schema(adminSchema)
        .csv(ADMIN_TWO_PATH)

    val postalCodes = sqlContext.read
        .option("header", "false")
        .option("delimiter", "\t")
        .schema(postalCodeSchema)
        .csv(POSTAL_CODE_PATH)
        .dropDuplicates(Seq("name", "country", "admin1_code"))    

    geonames.createOrReplaceTempView("geonames")
    countries.createOrReplaceTempView("countries")
    adminOne.createOrReplaceTempView("adminOne")
    adminTwo.createOrReplaceTempView("adminTwo")
    postalCodes.createOrReplaceTempView("postalCodes")

    val cities = sqlContext.sql(
          "SELECT g.geonameid, g.name, g.alternatenames, g.latitude, g.longitude, g.population, c.name as country_name, g.country as country_code, a1.name as admin1_name, g.admin1 as admin1_code, a2.name as admin2_name, p.postal_code " +
          "FROM geonames g " + 
          "LEFT OUTER JOIN countries c ON g.country = c.iso_alpha2 " + 
          "LEFT OUTER JOIN adminOne a1 ON g.adminOneCode = a1.code " +
          "LEFT OUTER JOIN adminTwo a2 ON g.adminTwoCode = a2.code " +
          "LEFT OUTER JOIN postalCodes p ON g.name = p.name AND g.country = p.country AND g.admin1 = p.admin1_code")
        .dropDuplicates(Seq("geonameid"))
        .cache()

    cities.createOrReplaceTempView("cities")

    // DATA INTEGRITY TEST
    val frenchCities = cities.filter($"country_code"==="FR").cache()
    assert(frenchCities.count() > 33000 && frenchCities.count() < 36000, "French cities count should be between 33000 and 36000")

    sqlContext.sql("SELECT * FROM cities WHERE name='Paris'").show()

    sc.stop()
  }
}
