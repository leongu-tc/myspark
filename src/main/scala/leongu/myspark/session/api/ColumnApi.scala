package leongu.myspark.session.api

import org.apache.spark.sql.types.{DataType, DecimalType}

object ColumnApi {

  case class Record(key: Int, value: String)

  def main(args: Array[String]) {

    val dType: DataType = DecimalType(24, 6)

    dType match {
      case DecimalType() => println("111111")
      case _ => println("2222222")
    }

    println("done!")
  }
}
