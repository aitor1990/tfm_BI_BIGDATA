import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
var tour = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
                option("header", "true").  // Does the file have a header line?
                option("delimiter", "\t"). // Set delimiter to tab or comma.
                load("/data/urb_ctour.tsv")

tour = tour.withColumn("split",split(col("indic_ur,cities\\time"), ",")).
        select(
            col("split")(0).as("variable"),col("split")(1).as("region"),
            col("2018 ").as("2018"),col("2017 ").as("2017"),col("2016 ").as("2016"),
            col("2015 ").as("2015"),col("2014 ").as("2014"),col("2013 ").as("2013"),
            col("2012 ").as("2012"),col("2011 ").as("2011"),col("2010 ").as("2010"),
            col("2009 ").as("2009"),col("2008 ").as("2008"),col("2007 ").as("2007"),
            col("2006 ").as("2006"),col("2005 ").as("2005"),col("2004 ").as("2004"),
            col("2003 ").as("2003"),col("2002 ").as("2002"),col("2001 ").as("2001"),
            col("2000 ").as("2000"),col("1999 ").as("1999"),col("1998 ").as("1998"),
            col("1997 ").as("1997"),col("1996 ").as("1996"),col("1995 ").as("1995"),
            col("1994 ").as("1994"),col("1993 ").as("1993"),col("1992 ").as("1992"),
            col("1991 ").as("1991"),col("1990 ").as("1990")
       )
tour.select("variable","region","2018").union(tour.select("variable","region","2017")).show()
/*var countries = spark.read.format("csv").
                           option("header", "true").
                           option("delimiter", ",").
                           load("/data/regions.csv").
                           select(col("UA_CODE_2017"),col("NAME"))
tour = tour.join(countries).
            where($"region" === $"UA_CODE_2017").
            drop("UA_CODE_2017","region")

tour.show()*/
