import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
var tour = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
                option("header", "true").  // Does the file have a header line?
                option("delimiter", "\t"). // Set delimiter to tab or comma.
                load("/data/raw/urb_ctour.tsv")

// clean column names
tour = tour.withColumn("split",split(col("indic_ur,cities\\time"), ",")).
        select(
            col("split")(0).as("variable"),col("split")(1).as("region"),
            $"2018 ".as("2018"),$"2017 ".as("2017"),$"2016 ".as("2016"),
            $"2015 ".as("2015"),$"2014 ".as("2014"),$"2013 ".as("2013"),
            $"2012 ".as("2012"),$"2011 ".as("2011"),$"2010 ".as("2010"),
            $"2009 ".as("2009"),$"2008 ".as("2008"),$"2007 ".as("2007"),
            $"2006 ".as("2006"),$"2005 ".as("2005"),$"2004 ".as("2004"),
            $"2003 ".as("2003"),$"2002 ".as("2002"),$"2001 ".as("2001"),
            $"2000 ".as("2000"),$"1999 ".as("1999"),$"1998 ".as("1998"),
            $"1997 ".as("1997"),$"1996 ".as("1996"),$"1995 ".as("1995"),
            $"1994 ".as("1994"),$"1993 ".as("1993"),$"1992 ".as("1992"),
            $"1991 ".as("1991"),$"1990 ".as("1990")
       )

var city_dimension = spark.read.parquet("/data/datawarehouse/city_dimension.parquet")


var variables = spark.read.format("csv").
                      option("header", "true").
                      option("delimiter", ",").
                      load("/data/raw/urb_esms_an2.csv").
                      select($"CODE".as("variable_code"),$"LABEL".as("variable_name"))
variables = variables.withColumn("index_variable",monotonically_increasing_id())

// change year from matrix to one column
tour = tour.select("variable","region","2018").withColumn("year",lit("2018")).
    union(tour.select("variable","region","2017").withColumn("year",lit("2017"))).
    union(tour.select("variable","region","2016").withColumn("year",lit("2016"))).
    union(tour.select("variable","region","2015").withColumn("year",lit("2015"))).
    union(tour.select("variable","region","2014").withColumn("year",lit("2014"))).
    union(tour.select("variable","region","2013").withColumn("year",lit("2013"))).
    union(tour.select("variable","region","2012").withColumn("year",lit("2012"))).
    union(tour.select("variable","region","2011").withColumn("year",lit("2011"))).
    union(tour.select("variable","region","2010").withColumn("year",lit("2010"))).
    union(tour.select("variable","region","2009").withColumn("year",lit("2009"))).
    union(tour.select("variable","region","2008").withColumn("year",lit("2008"))).
    union(tour.select("variable","region","2007").withColumn("year",lit("2007"))).
    union(tour.select("variable","region","2006").withColumn("year",lit("2006"))).
    union(tour.select("variable","region","2005").withColumn("year",lit("2005"))).
    union(tour.select("variable","region","2004").withColumn("year",lit("2004"))).
    union(tour.select("variable","region","2003").withColumn("year",lit("2003"))).
    union(tour.select("variable","region","2002").withColumn("year",lit("2002"))).
    union(tour.select("variable","region","2001").withColumn("year",lit("2001"))).
    union(tour.select("variable","region","2000").withColumn("year",lit("2000"))).
    union(tour.select("variable","region","1999").withColumn("year",lit("1999"))).
    union(tour.select("variable","region","1998").withColumn("year",lit("1998"))).
    union(tour.select("variable","region","1997").withColumn("year",lit("1997"))).
    union(tour.select("variable","region","1996").withColumn("year",lit("1996"))).
    union(tour.select("variable","region","1995").withColumn("year",lit("1995"))).
    union(tour.select("variable","region","1994").withColumn("year",lit("1994"))).
    union(tour.select("variable","region","1993").withColumn("year",lit("1993"))).
    union(tour.select("variable","region","1992").withColumn("year",lit("1992"))).
    union(tour.select("variable","region","1991").withColumn("year",lit("1991"))).
    union(tour.select("variable","region","1990").withColumn("year",lit("1990"))).
    select($"variable",$"region",$"2018".as("value"),$"year")

//filter values
tour = tour.filter(!$"value".contains(":")).
            withColumn("value",regexp_replace($"value", "\\s+", "")).
            withColumn("value",regexp_replace($"value", "\\D+", ""))
//create index for facts table
tour = tour.join(city_dimension.select("city_code","index_city")).
               where($"region" === $"city_code")


tour = tour.join(variables.select("variable_code","index_variable")).
            where($"variable_code" === $"variable").
            drop("variable")


var cinema_seats = tour.filter($"variable" === lit("CR1003I")).withColumnRenamed("variable","cinema_seats")

var beds_tour = tour.filter($"variable" === lit("CR2010I")).
                  select($"region".as("region_beds"),$"year".as("year_beds"),$"value".as("beds"))

var nights_spend = tour.filter($"variable" === lit("CR2011I")).
                   select($"region".as("region_nights"),$"year".as("year_nights"),$"value".as("nights"))
//join all columns to create one fact table
tour = cinema_seats.join(beds_tour).
            where($"region" === $"region_beds" && $"year" === $"year_beds").
            drop("region_beds","year_beds").
            join(nights_spend).
            where($"region" === $"region_nights" && $"year" === $"year_nights").
            select($"index_city",$"index_variable",$"value".as("cinema_seats"),$"beds",$"nights",$"year")

//cast variables to the needed format for the datawarehouse
tour = tour.withColumn("index_city", tour("index_city").cast(IntegerType)).
            withColumn("index_variable", tour("index_variable").cast(IntegerType)).
            withColumn("cinema_seats", tour("cinema_seats").cast(IntegerType)).
            withColumn("beds", tour("beds").cast(IntegerType)).
            withColumn("nights", tour("nights").cast(IntegerType)).
            withColumn("year", tour("year").cast(IntegerType))

tour.repartition($"year").write.mode("overwrite").parquet("/data/datawarehouse/tourism_facts.parquet")
