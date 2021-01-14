import java.nio.file.Paths
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
import scala.util.Random

/**
 * RDD
 *      - lower-level api
 *      - partitioned collection of records that can be operated on in parallel
 *      - dataframe, dataset -> compile -> rdd
 *      - 스파크의 structured api 의 최적화가 적용되지 않음.
 *      
 *  RDD 의 종류.
 *      - generic rdd
 *      - key-value rdd (*)
 *          - 키을 기준으로 분산된 데이터에 대한 특징적 연산을 지원.
 * 
 *  RDD 의 특징.
 *      1. a list of partitions. (RDD has one or more partitions)
 *      2. a function for computing each split  (RDD 에 대한 연산.)
 *      3. a list of dependencies on other RDDs (다른 RDD 에 대한 의존성)
 *      4. Partitioner (hash function - key-value rdd)
 *  
 *  RDD 생성.
 *      df.rdd
 *      rdd.toDF
 *      spark.createDataFrame(rdd, schema)  - rdd to df
 *      spark.sparkContext.parallelize(collection, partitions) - local collection to rdd
 *      
 *  Shared variables.
 *      - accumulator, broadcast variables
 *  
 *  체크포인트 (checkpoint) 개념.
 *      - 어떤 연산이 진행중인 rdd, dataframe 에 대해서
 *      중간 지점을 미래의 어느 시점에 처음이 아닌 그 시점 이후부터 사용할 수 있도록
 *      디스크에 저장 (결과물 혹은 인덱스).
 *  
 *  Key-Value RDD
 *      - 키-값 형태을 가지는 rdd.
 *      - 연산 메서드 <연산>ByKey
 *      - "키의 중복 여부" 를 허용.
 *      
 *      1. rdd to PairRDD
 *      2. transformation
 *          2-1. mapValues, flatMapValues
 *          2-2. rdd.keys, rdd.values, rdd.lookup(key)
 *      3. aggregation
 *          groupByKey
 *              - 키를 기준으로 모든 값을 집계
 *              - groupByKey 은 키를 기준으로 나누어진 파티션을 모두 메모리에 올림.
 *              - skew 키에 의해 파티션이 되었다면 OutOfMemoryErrors 발생할 수 있음.
 *              
 *          reduceByKey
 *              - groupByKey 와 달리 키를 기준으로 나누어진 파티션을 메모리에 올리지 않음.
 *          
 *          aggregate.
 *              - aggregate(startValue)(reducerInPartition, reducerInNodes)
 *              reducerInPartition - 파티션 내부의 집계 함수.
 *              reducerInNodes - 모든 파티션의 집계 함수.
 *      
 *      4. join
 *          join,
 *          fullOuterJoin,
 *          leftOuterJoin,
 *          rightOuterJoin,
 *          cartesian
 *          
 *      5. controlling partitions.
 *          coalesce
 *              - 같은 노드에 있는 복수의 파티션을 셔플. 
 *                  (repartition 과 다르게 네트웍 트래픽이 발생하지 않음) 
 *
 */
object RDDExample {
    
    def section(sec: String = "") = {
        println(s"========================${sec}========================")
    }
    
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("rdd")
                .getOrCreate()
        import spark.implicits._
        val sparkContext = spark.sparkContext

        /**
         * Creating RDD
         *  1. rdd from dataset
         *      dataset.rdd
         *  2. dataset from rdd   
         *      spark.createDataFrame(rdd, Schema)
         *      rdd.toDF()
         *  3. rdd from local collection    
         *      spark.sparkContext.parallelize(collection)
         *  4. input to rdd
         *      spark.sparkContext.textFile(file)   => line 당 하나의 record
         *      spark.sparkContext.wholeTextFile(dir) => 파일 당 하나의 record
         */
        val flights = spark.read
                .option("inferSchema", true)
                .option("header", true)
                .option("mode", "failfast")
                .option("dataFormat", "yyyy-MM-dd hh:mm:ss")
                .csv(s"${SparkData.PATH}/retail-data/by-day/*.csv")
        
        section("creating rdd.")
        //df to rdd
        val rdd: RDD[Row] = flights.limit(10).rdd
        rdd.foreach(r => println(r.get(0)))
        
        //rdd to df
        val df = rdd.map(r => 
                (r.getAs[String]("InvoiceNo"), 
                        r.getAs[String]("StockCode"),
                        r.getAs[Integer]("Quantity"),
                        r.getAs[Double]("UnitPrice")))
                .toDF()
        df.show()
        
        //collection to rdd
        val collection = "spark the definitive guide : big data processing made simple".split("\\s+")
        var wordRDD = spark.sparkContext.parallelize(collection, 2)
        wordRDD.foreach(r => println(r))
        
        //input to rdd
        val path = Paths.get("./src/main/resources/words.txt")
        wordRDD = spark.sparkContext.textFile(path.toString, 2)
                .flatMap(line => line.split("\\s+"))
        wordRDD.foreach(r => println(r))

        /**
         * transformations
         *  1. distinct
         *      unique rows
         *  2. filter
         *      where clause
         *  3. map
         *      map to new row
         *  4. flatMap
         *      flattening stream
         *          df..      /df[String]
         *             .map(w => w.split(""))   // df[Seq[String]]
         *             .flatMap(seq => seq)     // flatterning - df[String]   
         * 5. sortBy
         *      키에 대해서 수치화 시켜 그것을 기준으로 정렬. ascending
         */
        section("distinct")
        val cnt = wordRDD.distinct().count()
        println(cnt)

        section("filter")
        val prefix = "s";
        wordRDD.filter(w => w.startsWith(prefix)).foreach(w => println(w))
        
        section("map")
        val rows = wordRDD.map(w => (w, w(0), w.startsWith(prefix))).take(5)
        for (r <- rows)
            println(r)
        
        section("flatMap")
        wordRDD.map(w => w.split(""))
                .flatMap(w => w)
                .foreach(w => println(w))
        
        section("sort")
        wordRDD.sortBy(w => w.length * -1).foreach(w => println(w))

        /**
         * actions.
         *  execute plans (transformation)
         *  
         *  집계 연산. (aggregations)
         *  
         *  1. 리듀스 연산.
         *      리듀스 함수. (left, right) => new value
         *      주어진 두 개의 입력값을 하나로 합계.
         * 
         *  2. count.
         *      집계 연산.
         *      
         *  3. first, max, min, take
         */
        
        val length = wordRDD.map(w => w.length).reduce((left, right) => left + right)
        println(length)
        val maxWord = wordRDD.reduce((left, right) => if(left.length < right.length) right else left)
        println(maxWord)

        /**
         * operation on each partition
         * 
         *  mapPartitions
         *      각 파티션 기준으로의 데이터에 대해서 map 연산.
         *      (다른 말로 클러스터의 각 노드에서 연산을 따 수행)
         *  
         *  mapPartitionsWithIndex
         *      mapPartitions 과 같지만 각 파티션에 인덱스가 주어짐.
         *  
         *  foreachPartition
         *      mapPartitions 과 같지만 action 수행.
         *      
         */
        
        def onPartition(words: Iterator[String]): Iterator[String] = {
            val join = words.mkString(",")
            Iterator[String](join)
        }
        wordRDD.repartition(3).mapPartitions(onPartition).foreach(w => println(w))

        def onPartitionByIndex(index: Int, words: Iterator[String]): Iterator[(Int, String)] = {
            words.toList.map(w => (index, w)).iterator
        }
        wordRDD.repartition(3).mapPartitionsWithIndex(onPartitionByIndex).foreach(w => println(w))

        /**
         * rdd to key-value rdd
         *  1. rdd.map(r => (key, value))
         *  2. keyBy
         */
        section("PairRDD")
        wordRDD.map(r => (r.length, r)).foreach(r => println(r))
        wordRDD.map(r => (r.toLowerCase(), 1)).foreach(r => println(r))
        wordRDD.keyBy(r => r.charAt(0).toLower).foreach(r => println(r))

        /**
         * mapping over values
         *  
         *  mapValues.
         *      - map only for values
         *      
         *   flatMapValues
         *      - map and flattening values 
         */
        section("mapValues")
        val keywords = wordRDD.map(r => (r.charAt(0).toLower, r))
        keywords.mapValues(values => values.toUpperCase()).foreach(pair => println(pair))
        section("")
        keywords.flatMapValues(word => word.toUpperCase).foreach(pair => println(pair))
        
        /**
         * extracting keys and values
         *  rdd.keys    => Key RDD
         *  rdd.values  => Value RDD
         *  rdd.lookup(key)
         */
        section("keys & values")
        keywords.keys.foreach(k => println(k))
        section()
        keywords.values.foreach(v => println(v))
        section()
        val values = keywords.lookup('s')
        for (v <- values)
            println(v)

        /**
         * aggregations
         *      
         *      countByKey
         *          - how many values for each key?
         *      groupByKey
         *          - collecting all the values for each key
         *      reduceByKey
         *          - reduce values for each key
         *      aggregate(startValue)(reducerInPartition, reducerInPartitions) (for RDD)
         *          - rdd 집계
         *      aggregateByKey(startValue)(reducerInPartition, reducerInPartitions) (for Pair RDD)
         *          - key 을 기준으로 집계 (group by aggregation)
         *      combineByKey  
         *          combinedByKey(
         *              valCombiner,        // 초기 val.
         *              mergeValuesCombiner,    // combiner + val
         *              mergeCombinersCombiner, // combiner + combiner
         *              partition)
         */
        val pairs = wordRDD.flatMap(w => w)
                .map(w => (w, 1))
        
        section()
        pairs.countByKey().foreach(p => println(p))
        val nums = spark.sparkContext.parallelize(1 to 30, 5)
        
        section("groupByKey")
        pairs.groupByKey()
                .foreach(pair => println(pair))
        
        section()
        pairs.groupByKey()
                .map(pair => (pair._1, pair._2.sum))
                .foreach(p => println(p))
        
        section("reduceByKey")
        pairs.reduceByKey((left, right) => left + right).foreach(v => println(v))
        
        section("aggregate")
        val sum = nums.aggregate(0)((left, right) => if (left<right) right else left, (left, right) => left + right)
        println(sum)
        
        section("aggregateByKey")
        nums.map(e => (if (e % 2 == 0) true else false, e))
                .aggregateByKey(0)((left, right) => if (left<right) right else left, (left, right) => left + right)
                .foreach(e => println(e))
        pairs.aggregateByKey(0)((left, right) => left + right, (left, right) => if (left<right) right else left)
                .foreach(e => println(e))

        /**
         * joins
         *  leftRDD.join(rightRDD, numberOfPartitions)  
         *  
         *  inner join, outer join - 매칭되지 않은 키에 대한 처리.
         *  left, right, outer - 두 집합 (왼쪽, 오른쪽) 중 어느쪽에 합칠지에 대한 처리.
         *  cartesian - left * right
         *  
         */
        section("joins")
        val charMap = wordRDD.flatMap(w => w.toLowerCase())
                .distinct()
                .map(c => (c, new Random().nextDouble()))
        val partitions = 10
        pairs.join(charMap, partitions)
                .foreach(p => println(p))

        /**
         * partitions
         *  1. coalesce
         *      collapses partitions on the same work. (no network traffic)
         */
        val numOfPartitions = wordRDD.coalesce(1).getNumPartitions
        println(numOfPartitions)
    }
}
