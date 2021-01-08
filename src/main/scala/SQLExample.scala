import org.apache.spark.sql.{Row, SparkSession}

/**
 * Spark SQl
 *  run sql queries against views or tables.
 *  
 *  Hive - 빅데이터 SQL 서비스 레이어.
 *      SQL 쿼리를 빅데이터에 실행.
 *      세션에 걸쳐서 (글로벌) 테이블의 정보을 저장. (Hive metastores)
 *      
 *      SparkSession.enableHiveSupport
 *          - hive 메타스토어 활성화
 *      spark.hadoop.hive.metastore.warehouse.dir
 *          - embedded 메타스토어 사용.
 *          
 *  Spark & Hive
 *      Spark SQL 에서는 테이블의 정보와 메타 데이터를 읽기 위해
 *      Hive metastores 에 접근.
 *      File Listing 에 대한 비용 감소. 
 *  
 *      spark.sql.hive.metastore.version
 *          - 사용하는 hive metastore 버전.
 *      spark.sql.hive.metastore.jars
 *          - HiveMetastoreClient 초기화를 변경하기 위한 jar 설정.
 *          
 *  Catalog.
 *      org.apache.spark.sql.catalog.Catalog
 *      SQL 처리 추상화.
 *          1. 테이블 메타 정보 저장.
 *          2. 테이블, 뷰, 함수 정보 저장.
 *          
 *  Database & Table          
 *      Table 은 처리시에 DataFrame 과 유사하지만
 *      Table 은 데이터베이스에 저장되고 (영구적) DataFrame 은 해당 세션에서만 유효.
 *      
 *      기본적으로 테이블은 default database 에 저장.
 *      * saveAsTable 을 사용해야 managed table 이 생성. (cluster 에 걸쳐서 유효한 테이블)
 *      ? sparks.sql 로 테이블을 생성하면 세션 스코프?
 *      
 *      Database 은 테이블을 조직 & 관리하는 툴.
 *      Database 을 지정하지 않으면 기본적으로 default 을 사용.
 *      
 *  Spark-Managed Tables 
 *      table 은 기본적으로 metadata 와 데이터를 저장.
 *      
 *      DataFrame 의 saveAsTable 을 호출시 managed table 이 생성.
 *      
 *      spark.sql.warehouse.dir
 *          - default Hive warehouse 의 위치.
 *          
 *  Views
 *      기존 테이블의 mirror.
 *      스코프는 global, database, session
 *      
 *   Spark SQL.
 *      1. create
 *      2. insert
 *      3. drop
 *      4. show tables
 *      5. describe table
 *      6. show partitions
 *      7. refresh table
 *      
 *  Complex type.
 *      1. structs
 *          select (cols..)
 *          
 *      2. list
 *          collect_list
 *      3. functions
 *  
 *  subquery
 *      query within other query
 *      
 *      subquery 의 두 종류.
 *          1. correlated subqueries
 *              외부 스코프 쿼리에서 정보를 이용해 내부 쿼리를 실행.
 *              
 *          2. uncorrelated subqueries
 *              외부 스코프 쿼리 와 내부 쿼리간의 의존성이 없음.
 *
 * */
object SQLExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("sql")
                .enableHiveSupport()    // enable hive metastore & location
                .getOrCreate()
        import spark.implicits._
        
        var flights = spark.read.option("header", true)
                .option("inferSchema", true)
                .option("mode", "failfast")
                .option("dateFormat", "yyyy-MM-dd hh:mm")
                .csv(s"${SparkData.PATH}/flight-data/csv/2015-summary.csv")
        
        flights = flights.withColumnRenamed("dest_country_name", "dest")
                .withColumnRenamed("origin_country_name", "origin")
        flights.createOrReplaceTempView("flights_view")
        
        /**
         * spark sql interface
         *  spark.sql
         *  
         *  DataFrame and Table interoperation
         *  
         *  dataframe -> table -> dataframe
         *  
         *  sql 팁.
         *      """sql""" 
         *      다중 라인 허용.
         */
        spark.sql("select 1 + 1").show()
        
        val country = "Canada"
        val stats = spark.sql(s"""
                         select dest, count(dest) as counts, sum(count) as sums from flights_view 
                         group by dest 
                         having sums > 10 or upper(dest) like upper('%${country}')
                         order by sums desc
                         """)
        stats.show()
        spark.sql("drop view default.flights_view").show()

        /**
         * spark table (session scope)
         * 
         *  spark.sql.warehouse.dir
         *      테이블의 저장 위치. (Hive warehouse location)
         *  show tables in [db name]
         *      테이블 정보 쿼리.
         *      
         *  테이블 생성
         *      input 으로 부터
         *          create table [tableName] (col type, col type...) 
         *              using [format] 
         *              partition by (cols..)
         *              options (path 'location')
         *      DataFrame 으로 부터    
         *          create table if not exists [table] as select * from [sourceTable]
         *          
         */
        spark.sql("show tables in default").show()
        
        spark.sql(
            s"""
              create table if not exists flights (dest_country_name String, origin_country_name String, count Long) 
                using csv 
                partitioned by (dest_country_name)
                options (header true, path '${SparkData.PATH}/flight-data/csv/*.csv')
            """)
        println(spark.conf.get("spark.sql.warehouse.dir"))  //hive locations
        
        spark.sql("show tables in default").show()
        spark.sql("select * from flights").show()

        val df = Seq(
            (country, "Korea", 100L),
            (country, "Japan", 100L)
        ).toDF("dest_country_name", "origin_country_name", "count")
        df.createOrReplaceTempView("flights2")
        
        /**
         * insert
         *  
         *  insert into [table]
         *      partition (col=value)
         *      select cols from [sourceTable]
         *      
         *  * partition 사용시, select 절에서 partition column 은 제외시켜야 함.
         */
        spark.sql(
            s"""
              insert into flights partition (dest_country_name='${country}')
              select origin_country_name, count from flights2
              where dest_country_name = '${country}'
              """)
        spark.sql(
            s"""
               select * from flights 
                where dest_country_name = '${country}'
            """)
                .show()

        /**
         * Describing * Partitions.
         *  
         *  describe table [tableName]
         *      - 테이블의 메터데이터 정보
         *  show partitions [tableName] 
         *      - 테이블의 파티션 정보
         */
        spark.sql(s"describe table flights").show()
        spark.sql(s"show partitions flights").show()
        
        /**
         * drop table
         *  테이블과 테이블의 데이터를 삭제.
         */
        spark.sql("show tables in default").show()

        /**
         * views
         *  create or replace [temp|global] view [viewName]
         *      as select * from [sourceTable]
         *      
         *  temp.
         *     세션 스코프 & 현재 spark 앱 접근 가능.
         *  global.
         *     세션 스코프 & 모든 spark 앱이 접근 가능.
         *      
         */
        spark.sql(s"create or replace view flights_view as select * from flights where dest_country_name = '${country}'")

        /**
         * database
         *  
         *  1. create database
         *  2. show databases
         *  3. use [database]
         *  4. show tables in [database]
         *  5. select current_database()
         *  
         */
        spark.sql("create database test")
        spark.sql("show databases").show()
        spark.sql("use test")
        spark.sql(s"create table if not exists flights as select * from default.flights where dest_country_name = '${country}'")
        spark.sql("show tables in test").show()
        spark.sql("select * from flights").show()
        spark.sql("select current_database()").show()
        spark.sql("drop table if exists flights")
        spark.sql("drop database if exists test")
        spark.sql("use default")
        
        /**
         * case statement
         * 
         * select
         *  case when [condition] then [return]
         *       when [condition] then [return]
         *       else [return] 
         *  end
         *  from [table]
         */
        spark.sql("select case when dest_country_name = 'UNITED STATES' then 1 when dest_country_name = 'Egypt' then 0 else -1 end from flights")
                .show()

        /**
         * struct, lists, functions
         * 
         * struct
         *      - select (cols..)
         *      - select struct.*, struct.[field]     
         *      
         * list, set
         *      - collect_list, collect_set
         *      - aggregation 함수
         */
        //create struct
        spark.sql("select * from flights").show()
        spark.sql("create or replace view complexes as select (dest_country_name, origin_country_name) as complex, count from flights")
        //query on struct
        spark.sql("select *, complex.*, complex.dest_country_name from complexes").show()
        //agg to collection
        spark.sql("select collect_list(complex.dest_country_name), collect_set(complex.origin_country_name) from complexes").show()

        /**
         * subqueries
         *  1. uncorrelated subqueries
         *      내부 쿼리는 외부 쿼리에게 어떠한 정보를 요청하지 않음.
         *  2. correlated subqueries  
         *      내부 쿼리는 실행하기 위해 외부 쿼리의 정보를 이용.
         */
        flights.createOrReplaceTempView("flights")
        spark.sql("select dest from flights group by dest order by sum(count) desc limit 5").show()
        
        spark.sql(
            """select * from flights 
               where origin in 
                (select dest from flights group by dest order by sum(count) desc limit 5) 
            """).show()   //having 에서만 agg 함수를 사용할 수 있다.
     
        spark.sql(
            """
              select * from flights f1
                where exists (select 1 from flights f2 where f1.origin = f2.origin) 
                and  exists (select 1 from flights f2 where f1.dest = f2.dest) 
            """).show()
    }
}
