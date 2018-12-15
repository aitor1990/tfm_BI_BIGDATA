import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
var tour = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
                option("header", "true").  // Does the file have a header line?
                option("delimiter", "\t"). // Set delimiter to tab or comma.
                load("/data/urb_ctour.tsv")

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
var countries = spark.read.format("csv").
                        option("header", "true").
                        option("delimiter", ",").
                        load("/data/regions.csv").
                        select($"UA_CODE_2017".as("city_code"),$"NAME".as("city_name"))
countries = countries.withColumn("index_country",monotonically_increasing_id())

var variables = spark.read.format("csv").
                      option("header", "true").
                      option("delimiter", ",").
                      load("/data/urb_esms_an2.csv").
                      select($"CODE".as("variable_code"),$"LABEL".as("variable_name")
variables = variables.withColumn("index_variable",monotonically_increasing_id())

tour = tour.select("variable","region","2018").
    union(tour.select("variable","region","2017")).
    union(tour.select("variable","region","2016")).
    union(tour.select("variable","region","2015")).
    union(tour.select("variable","region","2014")).
    union(tour.select("variable","region","2013")).
    union(tour.select("variable","region","2012")).
    union(tour.select("variable","region","2011")).
    union(tour.select("variable","region","2010")).
    union(tour.select("variable","region","2009")).
    union(tour.select("variable","region","2008")).
    union(tour.select("variable","region","2007")).
    union(tour.select("variable","region","2006")).
    union(tour.select("variable","region","2005")).
    union(tour.select("variable","region","2004")).
    union(tour.select("variable","region","2003")).
    union(tour.select("variable","region","2002")).
    union(tour.select("variable","region","2002")).
    union(tour.select("variable","region","2001")).
    union(tour.select("variable","region","2000")).
    union(tour.select("variable","region","1999")).
    union(tour.select("variable","region","1998")).
    union(tour.select("variable","region","1997")).
    union(tour.select("variable","region","1996")).
    union(tour.select("variable","region","1995")).
    union(tour.select("variable","region","1994")).
    union(tour.select("variable","region","1993")).
    union(tour.select("variable","region","1992")).
    union(tour.select("variable","region","1991")).
    union(tour.select("variable","region","1990")).
    select($"variable",$"region",$"2018".as("value"))

tour = tour.filter(!$"value".contains(":")).
            withColumn("value",regexp_replace($"value", "\\s+", "")).
            withColumn("value",regexp_replace($"value", "\\D+", ""))

tour = tour.join(countries).
               where($"region" === $"city_code").
               drop("region","city_code")
tour = tour.join(variables).
            where($"variable_code" === $"variable").
            drop("variable_code","variable")

tour.write.csv("/data/prueba.csv")
