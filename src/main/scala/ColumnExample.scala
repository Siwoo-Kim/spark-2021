import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnExample {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local")
                .appName("column")
                .getOrCreate()
        import spark.implicits._
        
        val col1 = col("someCol")
        val col2 = col("someCol")
        val col3 = $"someCol"
        val col4 = expr("someCol")
        println(col1 == col2)
        println(col2 == col3)
        println(col3 == col4)   // column is exp
        
        //explicit column references = for joining
        val expcCol = spark.range(10).toDF("num").col("num")
        println(expcCol == $"num")  //false
        
        // parsed as logical tree
        val calcCol = (((col("someCol") + 5) * 200) - 6) < col("otherCol")
     
        // traverse columns
        for (c <- Schemas.retailSchema)
            println(c)
    }
}
