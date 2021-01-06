import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array_contains

/**
 * 조인
 *  두 집합 (왼쪽, 오른쪽) 의 데이터를 특정 하나 이상의 키를 통해 병합.
 *  
 *  조인시의 경우의 수.
 *      1. 오른쪽의 집합을 왼쪽 집합으로 합침. left join
 *      2. 왼쪽 집합을 오른쪽의 집합으로 합침. right join
 *      3. 특정 키에 매칭되지 않은 데이터를 버림.   inner join
 *      4. 특정 키에 매칭되지 않은 데이터를 유지.   outer join
 *      5. left * right (cartesian) cross join
 *  
 *  명시적 컬럼 참조. (조인시 키 지정)
 *      df.col
 *      
 *  스파크 조인 전략.
 *      노드간 통신 전략. node-to-node communication strategy
 *          노드간 통신을 위한 셔플 연산 (노드간 물리적 데이터의 위치 교환) 을 수행됨.
 *          키를 통한 병합으로 인해, 네트웍 트래픽이 발생되어 비싼 비용이 요구됨.
 *          
 *          1. all-to-all 전략
 *              왼쪽, 오른쪽 테이블이 모두 클시 조인 연산은 모든 노드 전반에서 실행.
 *              
 *          2. broadcast 전략
 *              왼쪽 혹은 오른쪽 테이블이 메모리에 적재될 수 있을만큼 상대적으로 작을시의 전략.
 *              작은 테이블을 각 노드의 메모리에 적재한 후 조인 연산을 수행.
 *              
 *      각 노드간 연산 전략. per node computation strategy
 */
object JoinExample {

    def section(sec: String) = {
        println(s"========================${sec}========================")
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("join")
                .getOrCreate()
        import spark.implicits._
        val people = Seq(
            (0, "Bill Chambers", 0, Seq(100)),
            (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
            (2, "Michael Armburst", 1, Seq(250, 100)),
            (3, "Cathie Wood", -1, Seq()))
                .toDF("id", "name", "program", "sparkStatus")
        val programs = Seq(
            (0, "Masters", "School of Information", "UC Berkeley"),
            (2, "Masters", "EECS", "UC Berkeley"),
            (1, "Ph.D.", "EECS", "UC Berkeley"))
                .toDF("id", "degree", "department", "school")
        val statuses = Seq(
            (500, "Vice President"),
            (250, "PMC Member"),
            (100, "Contributor"))
                .toDF("id", "status")
        people.createOrReplaceTempView("people")
        programs.createOrReplaceTempView("programs")
        statuses.createOrReplaceTempView("statuses")

        /**
         * joinType = inner
         *  왼쪽 그리고 오른쪽에 키가 매칭되는 데이터를 병합.
         */
        section("inner join")
        var rightPrograms = programs.withColumnRenamed("id", "programId")
        var join = people.col("program") === rightPrograms.col("programId") 
        
        people.join(rightPrograms, join, "inner")
                .drop($"program")
                .show()
        
        /**
         * joinType = outer
         *  왼쪽 또는 오른쪽에 키가 매칭되면 데이터를 병합.
         */
        section("outer join")
        people.join(rightPrograms, join, "outer")
                .drop("program")
                .show()

        /**
         * joinType = left_outer
         *  왼쪽 또는 왼쪽 그리고 오른쪽에 키가 매칭되면 데이터를 병합.
         *  left || (left && right)
         */
        section("left outer join")
        people.join(rightPrograms, join, "left_outer")
                .drop("program")
                .show()
        
        //right || (left && right)
        people.join(rightPrograms, join, "right_outer")
                .drop("program")
                .show()

        /**
         * joinType = left_semi
         *  왼쪽 그리고 오른쪽에 키가 매칭되는 왼쪽 데이터만을 유지.
         *  
         *  anti = !semi
         */
        section("semi & anti")
        people.join(rightPrograms, join, "left_semi")
                .show()
        
        people.join(rightPrograms, join, "left_anti")
                .show()

        /**
         * joinType = cross
         *  왼쪽의 각각의 키을 오른쪽의 모든 키에 대응.
         * 
         *  spark.sql.crossJoin.enable - 크로스 조인 허용 여부 설정.
         */
        section("cross")
        people.crossJoin(rightPrograms)
                .show()

        /**
         * joins on complex type
         *  join expression 으로 boolean 을 리턴한다면 어떤 표현식도 조인으로 사용 가능.
         */
        section("join on complex type")
        people.withColumnRenamed("id", "personId")
                .join(statuses, array_contains($"sparkStatus", $"id"))
                .show()

        /**
         * 현재 조인 연산을 체크.
         *  check whether broadcast or all-to-all
         *  
         *  조인 힌트 join hint
         *      MAPJOIN, BROADCAST, BROADCASTJOIN
         */
        section("explain broadcast")
        people.join(rightPrograms, people.col("program") === rightPrograms.col("programId"), "inner")
                .explain()
    }
}
