import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 집계.
 *  - 다수의 자료를 "key" 을 기반 "그룹화" 이후 "집계 함수"를 통해 하나의 값을 획득.
 *  - 대부분, 수치 데이터 위주로 집계 수행.
 *  
 *  집계 방법.
 *      1. aggregation in a select statement.
 *          select 문에서의 집계 함수.
 *          root level 집계.
 *          
 *      2. group by
 *          group by 문에서 하나 이상의 키와 집계 함수.
 *          group level 집계.
 *          
 *      3. window
 *          window 함수와 하나 이상의 키와 집계 함수.
 *          group by 와의 차이점은 집계 결과는 행과 행사이의 관계에서 연산을 토대로 도.
 *          
 *      4. grouping set.
 *          집계를 여러 다른 level 에서 수행. (rollup & cube)
 * 
 *      5. user-defined aggregation function (UDAF)
 *          1. input schema -> input's StructType
 *          2. buffer schema    -> ouput's StuctType
 *          
 *  RelationalGroupedDataset
 *      - grouping 의 결과, 이 객체를 통해 집계 수행을 요청.
 *  
 *  aggregation functions.
 *      1. count - 로우의 갯수 반환.
 *      2. first & last - 첫번째 혹은 마지막 로우 반환.
 */
object AggregationExample {
    
    def section(sec: String) = {
        println(s"========================${sec}========================")
    }
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("aggregation")
                .getOrCreate()
        import spark.implicits._
        
        var retail = spark.read
                .option("header", true)
                .option("inferSchema", true)
                .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
                .option("mode", "failfast")
                .csv(s"${SparkData.PATH}/retail-data/all/*.csv")
        retail.cache()
        retail.createOrReplaceTempView("retail")

        /**
         *  count    - 로우의 갯수를 가운팅.
         *      count(*) 은 null 포함 카운팅.
         *      count(col) 은 null 제외 카운팅.
         *  countDistinct - 중복되지 않은 로우의 갯수를 카운팅.
         */
        section("simple agg")
        retail.select(
            count($"*"),
            count($"customerid"),
            countDistinct($"customerid")
        ).show()

        /**
         * first & last
         */
        retail.select(
            first($"stockcode"),
            last($"stockcode")
        ).show()

        /**
         * min & max & sum & avg & stddev & corr
         *  - 수치 데이터의 최소, 최대값, 합계, 평균, 
         *      표준편차 (평균값에서의 분산 정도), 
         *      상관계수 (연속된 두 변수간의 분산 정도: -1~0~1)
         */
        retail.select(
            min($"quantity"),
            max($"quantity"),
            sum($"quantity"),
            avg($"quantity"),
            (sum($"quantity") / count($"quantity")).as("mean"),
            stddev($"quantity"),
            corr($"invoiceno", $"quantity")
        ).show()

        /**
         * collect_set, collect_list
         *  특정 컬럼의 값을 set 혹은 list 로 집계
         */
        retail.select(collect_set($"country"), collect_list($"country")).show()
        
        /**
         * groupBy - 키를 기반으로 한 group level 집계
         *  1. grouping by keys
         *  2. aggregation function
         *  
         */
        section("groupBy")
        retail.groupBy($"invoiceno", $"customerid")
                .count()
                .show()
        
        retail.groupBy("invoiceno")
                .agg(
                    count($"quantity"),
                    max($"quantity"),
                    min($"quantity"),
                    sum($"quantity"))
                .show()

        /**
         * window 함수.
         *  특정 윈도우로 정의된 데이터 집합에 집계 함수를 수행.
         *  데이터는 하나 이상의 frame (그룹화된 데이터) 에 속할 수 있음.
         *      
         *      각 날짜마다 데이터를 집계. 이때 날짜를 기반으로 그룹화된 집합을 frame.
         *      
         *  1. frame
         *      윈도우의 정의에 따라 그룹화된 집합.
         *      
         *  2. window specification.
         *      어떤 행이 집계 함수에 전달될지 정의.
         *  
         *      partition by
         *          그룹키 방법 지정.
         *      ordering
         *          frame 안에서는 order 정의.
         *      rowBetween
         *          frame specification.
         *          현재 행에 바탕을 두어 프레임 안에서 어떠한 기준으로 집계를 할지 정의.
         *          
         *          unboundedPreceding - 윈도우의 시작 위치가 첫 번째 로우를 의미.
         *          unboundedFollowing - 윈도우의 시작 위치가 마지막 로우를 의미.
         *          currentRow  - 현재 로우까지를 의미.
         *
         *          rowsBetween(unboundedPreceding, unboundedFollowing) - frame 안에서 시작 로우부터 마지막 로우까지 집계값
         *          rowsBetween(unboundedPreceding, currentRow) - frame 안에서 시작 로우부터 현재 로우까지 집계값
         *          rowsBetween(currentRow, unboundedFollowing) - frame 안에서 현재 로우부터 마지막 로까지 집계값
         *          
         *  3. window function. 윈도우 함수.
         *      - rank
         *          rank & dense_rank
         *          rank 와 dense_rank 은 동등한 랭크로 인한 갭의 여부 차이.
         *      - agg
         */
        section("window")
        val timeFormat = "MM/d/yyyy H:mm"
        retail = retail.withColumn("date", to_date($"invoicedate", timeFormat))
        
        val windowSpec = Window.partitionBy($"customerid", $"date")
                .orderBy($"quantity".desc_nulls_last)
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        val maxQuantity = max($"quantity").over(windowSpec)
        val sumQuantity = sum($"quantity").over(windowSpec)
        val drankQuantity = dense_rank().over(windowSpec)
        val rankQuantity = rank().over(windowSpec)
        
        retail.where("customerId is not null")
                .orderBy("customerId")  // ordering over frames
                .select(
                    $"customerId",
                    $"date",
                    $"quantity",
                    maxQuantity.as("maxFrame"),
                    sumQuantity.as("sumFrame"),
                    drankQuantity.as("denserankFrame"),
                    rankQuantity.as("rankFrame")
                ).show(200)


        /**
         * UDAF (user defined agg function)
         *  - extends UserDefinedAggregateFunction
         *
         *  1. inputSchema - input StructType
         *  2. bufferSchema - intermediate result StructType
         *  abstract methods
         *  3. dataType - return DataType
         *  4. initialize - allow to init agg buffer
         *  5. update - allow update the buffer
         *  6. merge - agg two buffers
         *  7. evaluate - generate final result
         */
        section("UDAF")
        val booland = new BoolAnd
        spark.udf.register("booland", new BoolAnd)
        spark.range(1)
                .select(explode(array(lit(true), lit(true), lit(true))).as("t"))
                .select(explode(array(lit(true), lit(false), lit(true))).as("f"), $"t")
                .show()
        
        spark.range(1)
                .select(explode(array(lit(true), lit(true), lit(true))).as("t"))
                .select(explode(array(lit(true), lit(false), lit(true))).as("f"), $"t")
                .select(booland($"t"), booland($"f"))
                .show()
    }

    class BoolAnd extends UserDefinedAggregateFunction {
        override def inputSchema: StructType = StructType(StructField("value", BooleanType) :: Nil)
        override def bufferSchema: StructType = StructType(StructField("result", BooleanType) :: Nil)

        override def dataType: DataType = BooleanType

        override def deterministic: Boolean = true

        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = true
        }

        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getBoolean(0) && buffer2.getBoolean(0)
        }

        override def evaluate(buffer: Row): Any = {
            buffer(0)
        }
    }
}
