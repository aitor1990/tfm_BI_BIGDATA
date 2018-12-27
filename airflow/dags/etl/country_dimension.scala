import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
var cities = spark.read.format("csv").
                        option("header", "true").
                        option("delimiter", ",").
                        load("/data/regions.csv").
                        select($"UA_CODE_2017".as("city_code"),$"NAME".as("city_name"))
cities = cities.withColumn("index_city",monotonically_increasing_id())

var countries = spark.read.format("csv").
                        option("header", "true").
                        option("delimiter", " ").
                        load("/data/countries.csv").
                        select($"Code".as("country_code"),$"English".as("country_name"),$"ThreeCode".as("country_map_code"))

var city_dimension = countries.join(cities).
                                  where($"country_code" === substring($"city_code",0,2)).
                                  drop("country_code")
city_dimension = city_dimension.withColumn("index_city", city_dimension("index_city").cast(IntegerType))
city_dimension.write.mode("overwrite").parquet("/data/city_dimension.parquet")
