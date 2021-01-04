import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataFrameExample {

    def section(sec: String) = {
        println(s"========================${sec}========================")
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("dataframe")
                .getOrCreate()
        spark.conf.set("spark.sql.shuffle.partitions", 5)
        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
        
        /**
         * Create dataframe
         *  1. from raw source to Dataframe
         *  2. Rows or RDD to Dataframe
         *  
         *  JSON 은 데이터 로드시, 대소문자 구별
         */
        var flights = spark.read.schema(Schemas.flightSchema)
                .json(s"${SparkData.PATH}/flight-data/json/*.json")
        flights.createOrReplaceTempView("flights")
        
        val manualSchema = StructType(Array(
            StructField("some", StringType),
            StructField("col", StringType),
            StructField("names", LongType)))
        
        var rows = Seq(
            Row("hello", null, 1L),
            Row("spark", null, 2L))
        
        var rdd = spark.sparkContext.parallelize(rows)
        var df = spark.createDataFrame(rdd, manualSchema)
        df.show()

        var retail = spark.read
                .schema(Schemas.retailSchema)
                .option("header", true)
                .option("mode", "failfast")
                .csv(s"${SparkData.PATH}/retail-data/by-day/*.csv")

        retail.printSchema()
        retail.createOrReplaceTempView("retail")
        retail.show(5)
        
        /**
         * query on dataframe
         */
        flights = flights.select(
            $"dest_country_name".as("destination"), 
            $"origin_country_name".as("origin"), 
            $"count")
                .withColumn("within", $"destination" === $"origin")
        
        flights.where($"within").show()

        /**
         * simple agg
         */
        var agg = flights.select(avg($"count"), countDistinct($"destination"))
        agg.show()
        
        /**
         * literal
         * 
         *  native vs spark type
         *  lit -> native to spark type
         */
        flights.select($"*", lit(1).as("one")).show(2)
        flights.select(lit(5), lit("five"), lit(5.0)).show(2)

        /**
         * adding & renaming column
         */
        flights.withColumn("one", lit(1)).show()
        flights.withColumnRenamed("within", "withinCountry").show()

        /**
         * drop columns *
         */
        flights.drop("within", "count").show()
        for (c <- flights.drop("within", "count").schema)
            println(c)

        /**
         * casting columns
         */
        flights.select($"count".cast("Long")).show(2)

        /**
         * filtering (and)
         * 
         * 1. and (모든 filter 절의 chain 은 and 연산)
         * 2. or 
         * 3. ===, =!= (columns), =, <> (exp), <, >
         * 4. isin, contains
         * 5. boolean column
         */
        section("dataframe filtering")
        flights.where($"count" < 2)
                .where($"origin" =!= "Croatia")
                .show(2)
        
        flights.where(($"count"<2).or($"origin" =!= "Croatia"))
                .show(2)

        retail.where($"invoiceno" === 536365)
                .select($"invoiceno", $"description")
                .show(5, false)

        var filters = $"unitprice" > 600
        filters = filters.or($"description".contains("POSTAGE"))
        retail.where($"stockcode".isin("DOT"))
                .where(filters)
                .show(5, false)
        
        var filter = $"stockcode".contains("DOT")
        retail.withColumn("isExpensive", filter.and(filters))
                .where($"isExpensive")
                .show(5, false)
        
        retail.withColumn("isExpensive", not($"unitprice" <= 250))
                .where($"isExpensive")
                .select($"description", $"unitprice")
                .show(5, false)

        /**
         * numbers
         *  1. basic op (+,-,*,/)
         *  2. pow
         *  3. round(val, pos), bround(val, pos)
         *      round up        round down
         *  4. correlation coefficient (상관 관계, -1 ~ 0 ~ 1)
         *  5. describe for numeric columns - count, mean, stddev, max, min
         *  6. increasing id
         */
        section("dataframe numbers")
        val fabricatedQuantity = pow($"quantity" * $"unitprice", 2) + 5
        retail.select($"customerId", fabricatedQuantity.as("realQuantity")).show(2)
        
        df.select(
            round(lit(2.4)),    //down
            round(lit(2.5)),    //up
            round(lit(2.6)),    //up
            bround(lit(2.4)),   //down
            bround(lit(2.5)),   //down
            bround(lit(2.6)))   //up
                .show(2)
        
        retail.select(corr($"quantity", $"unitprice")).show()
        
        retail.describe().show()
        
        retail.select(monotonically_increasing_id().as("id"), $"*")
                .orderBy($"id".asc)
                .show()

        /**
         * strings
         * 
         * 1. initcap, lower, upper
         * 2. lpad, ltrim, rpad, rtrim, trim
         * 3. regexp_extract, regexp_replace
         * 4. translate (문자 단위로 해당 인덱스와 맞는 대체 문자 replace)
         * 5. contains
         */
        //capitalize
        section("dataframe strings")
        retail.select(
            initcap($"description"),
            lower($"description"),
            upper($"description")
        ).show(5, false)

        //pading
        val hello = "HELLO"
        val spaceHello = s"   ${hello}   "
        df.select(
            trim(lit(spaceHello)),
            ltrim(lit(spaceHello)),
            rtrim(lit(spaceHello)),
            lpad(lit(hello), 3 + hello.length, "*"),
            rpad(lit(hello), 3 + hello.length, "*"),
            rpad(lpad(lit(hello), 3  + hello.length, "*"), 3 + 3 + hello.length, "*")
        ).show()
        
        //regexp & search
        val colors = Seq("black", "white", "red", "green", "blue")
        var regexp = colors.map(_.toUpperCase).mkString("|")
        retail.select(
            $"description",
            regexp_replace($"description", regexp, "COLOR").as("colorflag"))
                .where(regexp_extract($"description", regexp, 0) =!= "")
                .show()
        
        retail.select(
            $"description",
            translate(upper($"description"), "LEET", "1337").as("translate"))
                .show()
        
        regexp = colors.map(_.toUpperCase).mkString("(", "|", ")")
        retail.select(
            $"description",
            regexp_extract($"description", regexp, 1).as("extract")
        ).show()
        
        retail.withColumn(
            "hasColor", $"description".contains("BLACK").or($"description".contains("WHITE")))
                .where($"hasColor")
                .show()
        
        val filterSeq = colors.map(c => {
            $"description".contains(c.toUpperCase).as(s"is_$c")
        }) :+ $"*"
        
        retail.select(filterSeq:_*) //varargs
                .where($"is_red")
                .show()

        /**
         * date and timestamps
         * 
         * 1. current time
         *  - current_date
         *  - current_timestamp
         *  
         * 2. date manipulation
         *  - date_sub, date_add
         *  - date_diff, months_between
         *  
         * 3. conversion (string -> date or timestamp)
         *  - to_date, to_timestamp
         *  
         * 4. timezone
         *  spark.sql.session.timeZone
         *      - session local timezone
         */
        section("date & timestamp")
        spark.conf.set("spark.sql.session.timeZone", "GMT-5")
        df.select(current_timestamp()).show()
        
        spark.conf.set("spark.sql.session.timeZone", "GMT+9")
        df.select(current_timestamp()).show()
        
        val dateDF = spark.range(10).toDF("num")
                .withColumn("today", current_date())
                .withColumn("now", current_timestamp())
        dateDF.createOrReplaceTempView("dates")
        dateDF.printSchema()
        
        dateDF.select(
            date_sub($"today", 5), 
            date_add($"today", 5))
                .show()
        
        dateDF.select(
            $"today",
            date_sub($"today", 7).as("weekago"))
                .withColumn("diff", datediff($"today", $"weekago"))
                .show()
        
        dateDF.select(
            to_date(lit("2020-01-01")).as("start"),
            to_date(lit("2021-01-01")).as("end")
        ).withColumn("between", months_between($"end", $"start"))
                .show()
        
        val dateFormat = "yyyy-dd-MM"
        spark.range(1).select(
            to_date(lit("2017-12-11"), dateFormat),
            to_date(lit("2017-20-12"), dateFormat),
            to_timestamp(lit("2017-12-11"), dateFormat),
            to_timestamp(lit("2017-20-12"), dateFormat)
        ).show()

        /**
         * handling nulls
         *  - DataFrame.na
         *  
         *  fill null column
         *      1. coalesce
         *  
         *  drop null rows
         *      1. any, all
         *          any = drop rows in which any value is null
         *          all = drop rows in which all values are null
         *          
         *          DateFrame.na.drop("any")
         *  
         *  replace the value
         *      replace all values in a column according to their value
         */
        section("nulls (na)")
        spark.range(2)
                .select(coalesce(lit(null), lit("a"), lit("b")))
                .show()

        spark.range(1).toDF("num")
                .select(lit(null), lit("a"))
                .na
                .drop("any")
                .show()
        
        spark.range(1).toDF("num")
                .select(lit(null), lit("a"))
                .na
                .drop("all")
                .show()
        
        spark.range(10).toDF("num")
                .na.replace("num", Map(1 -> (1 << 3)))
                .show()

        /**
         * complex types
         *  1. structs
         *      - DataFrame
         *      - "." - accessing column
         *      - "*" - all columns to top level.
         *  2. arrays.
         *      - "split" - creating array from string
         *      - "[]" - accessing a column by index
         *      - size - length of the array
         *      - array_contains
         *      
         *      explode *
         *          - 배열인 컬럼 하나를 지정하고 explode 을 수행하면
         *          각 배열의 원소는 하나의 행 되고 나머지 값은 기존 값을 복제.
         *          - 주의할 점은 기존 df 의 행의 길이는 변경된다는 것.
         *  3. map
         *      - map()
         *      - "['키']" - accessing a column be key
         *      - explode - 배열과 같은 기능.
         *      
         */
        section("complex")
        retail.select(
            struct($"description", $"invoiceno").as("complex"), 
            $"*"
        ).select(
            $"complex",
            $"complex.description",
            $"complex.invoiceno",
            $"complex.*"
        ).show(5)
        
        retail.select(split($"description", " ").as("array"))
                .select(
                    expr("array[0]"), 
                    expr("array[1]"), 
                    $"array")
                .withColumn("length", size($"array"))
                .withColumn("hasRed", array_contains($"array", "RED"))
                .show(10)
        
        retail.withColumn("array", split($"description", " "))
                .withColumn("explode", explode($"array"))
                .show(5)
        
        retail.select(map($"description", $"invoiceno").as("map"))
                .select(expr("map['WHITE METAL LANTERN']"), $"*")
                .show(5)
        
        /**
         * distinct rows
         */
        section("distinct")
        flights.select("origin", "destination").distinct().show(2)

        /**
         * union 
         * 유니온 연산. 
         *  수직 결합. (열의 길이가 늘어난다.)
         *  유니온 연산은 위치 기반으로 수행된다.
         *  
         * 조인 연산.
         *  수평 결합. (행의 길이가 늘어난다.) 
         *  조인 연산은 외래키 기반으로 수행된다.
         */
        val schema = flights.schema
        rows = Seq(Row("Canada", "Korea", 101L, false), Row("Korea", "Japan", 202l, false))
        rdd = spark.sparkContext.parallelize(rows)
        var subFlights = spark.createDataFrame(rdd, schema)
        flights = flights.union(subFlights)
                .where(($"origin" === "Korea").or($"destination" === "Korea"))

        /**
         * 1. sorting (handling null)
         * 2. sorting for optimization (다른 transformation 수행 전 성능 개선을 위해 정렬)
         */
        flights.orderBy($"count".desc_nulls_last, $"destination".desc_nulls_first)
                .show(5)
        
        flights.sortWithinPartitions($"destination", $"count")
                .groupBy($"destination")
                .agg(sum($"count"))
                .show()

        /**
         * repartition and coalesce
         * 
         * repartition = control the physical layout of data across cluster. (shuffle) (병렬성 증가, 네트웍 트래픽 성능 감소)
         * coalesce = combine partitions (병렬성 감소, 네트웍 트래픽 성능 증가)
         */
        flights.repartition(5,$"destination")
                .where($"destination" === "Korea")
                .coalesce(1)
                .show()
        
    }
}
