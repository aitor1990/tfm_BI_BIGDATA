import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
var financial = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
                option("header", "true").  // Does the file have a header line?
                option("delimiter", "\t"). // Set delimiter to tab or comma.
                load("/data/urb_clma.tsv")
//clean column names
financial = financial.withColumn("split",split(col("indic_ur,cities\\time"), ",")).
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

var city_dimension = spark.read.parquet("/data/city_dimension.parquet")


var variables = spark.read.format("csv").
                      option("header", "true").
                      option("delimiter", ",").
                      load("/data/urb_esms_an2.csv").
                      select($"CODE".as("variable_code"),$"LABEL".as("variable_name"))
variables = variables.withColumn("index_variable",monotonically_increasing_id())

// change year from matrix to one column
financial = financial.select("variable","region","2018").withColumn("year",lit("2018")).
    union(financial.select("variable","region","2017").withColumn("year",lit("2017"))).
    union(financial.select("variable","region","2016").withColumn("year",lit("2016"))).
    union(financial.select("variable","region","2015").withColumn("year",lit("2015"))).
    union(financial.select("variable","region","2014").withColumn("year",lit("2014"))).
    union(financial.select("variable","region","2013").withColumn("year",lit("2013"))).
    union(financial.select("variable","region","2012").withColumn("year",lit("2012"))).
    union(financial.select("variable","region","2011").withColumn("year",lit("2011"))).
    union(financial.select("variable","region","2010").withColumn("year",lit("2010"))).
    union(financial.select("variable","region","2009").withColumn("year",lit("2009"))).
    union(financial.select("variable","region","2008").withColumn("year",lit("2008"))).
    union(financial.select("variable","region","2007").withColumn("year",lit("2007"))).
    union(financial.select("variable","region","2006").withColumn("year",lit("2006"))).
    union(financial.select("variable","region","2005").withColumn("year",lit("2005"))).
    union(financial.select("variable","region","2004").withColumn("year",lit("2004"))).
    union(financial.select("variable","region","2003").withColumn("year",lit("2003"))).
    union(financial.select("variable","region","2002").withColumn("year",lit("2002"))).
    union(financial.select("variable","region","2001").withColumn("year",lit("2001"))).
    union(financial.select("variable","region","2000").withColumn("year",lit("2000"))).
    union(financial.select("variable","region","1999").withColumn("year",lit("1999"))).
    union(financial.select("variable","region","1998").withColumn("year",lit("1998"))).
    union(financial.select("variable","region","1997").withColumn("year",lit("1997"))).
    union(financial.select("variable","region","1996").withColumn("year",lit("1996"))).
    union(financial.select("variable","region","1995").withColumn("year",lit("1995"))).
    union(financial.select("variable","region","1994").withColumn("year",lit("1994"))).
    union(financial.select("variable","region","1993").withColumn("year",lit("1993"))).
    union(financial.select("variable","region","1992").withColumn("year",lit("1992"))).
    union(financial.select("variable","region","1991").withColumn("year",lit("1991"))).
    union(financial.select("variable","region","1990").withColumn("year",lit("1990"))).
    select($"variable",$"region",$"2018".as("value"),$"year")

//filter values
financial = financial.filter(!$"value".contains(":")).
            withColumn("value",regexp_replace($"value", "\\s+", "")).
            withColumn("value",regexp_replace($"value", "\\D+", "")).
            filter(!$"variable".contains(":"))
//create ratios
financial = financial.withColumn("value",$"value"/10)

//create index for facts table
financial  = financial.join(city_dimension.select("city_code","index_city")).
               where($"region" === $"city_code")


financial = financial.join(variables.select("variable_code","index_variable")).
            where($"variable_code" === $"variable").
            drop("variable")

// separate variables structured in one column to an specific column for each one
var activity_rate = financial.filter($"variable" === lit("EC1001I")).withColumnRenamed("variable","activity_rate")

var activity_rate_male = financial.filter($"variable" === lit("EC1002I")).
                  select($"region".as("region_activity_rate_male"),$"year".as("year_activity_rate_male"),$"value".as("activity_rate_male"))

var activity_rate_female = financial.filter($"variable" === lit("EC1003I")).
                   select($"region".as("region_activity_rate_female"),$"year".as("year_activity_rate_female"),$"value".as("activity_rate_female"))


var unem_rate = financial.filter($"variable" === lit("EC1020I")).
                        select($"region".as("region_unem_rate"),$"year".as("year_unem_rate"),$"value".as("unem_rate"))
var unem_rate_male = financial.filter($"variable" === lit("EC1011I")).
                       select($"region".as("region_unem_rate_male"),$"year".as("year_unem_rate_male"),$"value".as("unem_rate_male"))
var unem_rate_female = financial.filter($"variable" === lit("EC1012I")).
                  select($"region".as("region_unem_rate_female"),$"year".as("year_unem_rate_female"),$"value".as("unem_rate_female"))


var empl_agriculture = financial.filter($"variable" === lit("EC2008I")).
                        select($"region".as("region_empl_agriculture"),$"year".as("year_empl_agriculture"),$"value".as("empl_agriculture"))
var empl_industry = financial.filter($"variable" === lit("EC2009I")).
                       select($"region".as("region_empl_industry"),$"year".as("year_empl_industry"),$"value".as("empl_industry"))
var empl_construction = financial.filter($"variable" === lit("EC2022I")).
                  select($"region".as("region_empl_construction"),$"year".as("year_empl_construction"),$"value".as("empl_construction"))

//join all columns to create one fact table
financial = activity_rate.join(activity_rate_male).
            where($"region" === $"region_activity_rate_male" && $"year" === $"year_activity_rate_male").
            drop("region_activity_rate_male","year_activity_rate_rate_male").
            join(activity_rate_female).
            where($"region" === $"region_activity_rate_female" && $"year" === $"year_activity_rate_female").
            join(unem_rate).
            where($"region" === $"region_unem_rate" && $"year" === $"year_unem_rate").
            join(unem_rate_male).
            where($"region" === $"region_unem_rate_male" && $"year" === $"year_unem_rate_male").
            join(unem_rate_female).
            where($"region" === $"region_unem_rate_female" && $"year" === $"year_unem_rate_female").
            join(empl_agriculture).
            where($"region" === $"region_empl_agriculture" && $"year" === $"year_empl_agriculture").
            join(empl_industry).
            where($"region" === $"region_empl_industry" && $"year" === $"year_empl_industry").
            join(empl_construction).
            where($"region" === $"region_empl_construction" && $"year" === $"year_empl_construction").

            select($"index_city",$"index_variable",$"value".as("activity_rate"),
                  $"activity_rate_male",$"activity_rate_female",
                  $"unem_rate",$"unem_rate_male",$"unem_rate_female",
                  $"empl_agriculture",$"empl_industry",$"empl_construction",
                  $"year")
//cast variables to the needed format for the datawarehouse
financial = financial.withColumn("index_city", financial("index_city").cast(IntegerType)).
            withColumn("index_variable", financial("index_variable").cast(IntegerType)).
            withColumn("year", financial("year").cast(IntegerType)).
            withColumn("activity_rate", financial("activity_rate").cast(IntegerType)).
            withColumn("activity_rate_male", financial("activity_rate_male").cast(IntegerType)).
            withColumn("activity_rate_female", financial("activity_rate_female").cast(IntegerType)).
            withColumn("unem_rate", financial("unem_rate").cast(IntegerType)).
            withColumn("unem_rate_male", financial("unem_rate_male").cast(IntegerType)).
            withColumn("unem_rate_female", financial("unem_rate_female").cast(IntegerType)).
            withColumn("empl_agriculture", financial("empl_agriculture").cast(IntegerType)).
            withColumn("empl_industry", financial("empl_industry").cast(IntegerType)).
            withColumn("empl_construction", financial("empl_construction").cast(IntegerType))

financial.write.mode("overwrite").parquet("/data/labour_facts.parquet")
