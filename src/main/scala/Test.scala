import Typical.core.Typeable._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit, when}

object Test {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val rand = scala.util.Random

  implicit val startdf = Seq((1,2),(2,3),(3,4)).toDF("ONE","TWO")

  class ONE extends AXIOM[ONE]
  class TWO extends AXIOM[TWO]

val otmap: COL[ONE with TWO ] => Column = (ot:COL[ONE with TWO]) => {
  val one = ot.get[ONE]
  val two = ot.get[TWO]
  when(one.col.mod(2) === 0,two.col).otherwise(one.col)
}
  val threemap: COL[THREE] => Column = (three:COL[THREE]) => {
    when(three.col === "3", lit("is three")).otherwise("not three")
  }
  implicit val three = otmap.satisfy[THREE]
  implicit val four = threemap.satisfy[FOUR]

  class THREE extends Dependency[ONE with TWO,THREE]
  class FOUR extends Dependency[THREE,FOUR]
}