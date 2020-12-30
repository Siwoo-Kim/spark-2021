import org.apache.spark.sql.Row

object RowExample {

    def main(args: Array[String]): Unit = {
        val row1 = Row("Hello", null, 1, false)
        println(row1(0))
        println(row1(0).asInstanceOf[String])   // fetch & casting
        println(row1.getString(0))
        println(row1.getInt(2))
    }
}
