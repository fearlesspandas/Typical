import Typical.core.Typeable._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, when}

object Test {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val rand = scala.util.Random
  //startdf should come into the picture after joins on data have been performed
  implicit val startdf = Seq((1,2),(2,3),(3,4)).toDF("ONE","TWO")

  class ONE extends AXIOM[ONE]
  class TWO extends AXIOM[TWO]

  implicit val three: COL[ONE with TWO ] => COL[THREE] = (ot:COL[ONE with TWO]) => {
    val one = ot.getcol[ONE]
    val two = ot.getcol[TWO]
    class T extends COL[THREE](when(one.mod(2) === 0,two).otherwise(one))
    new T
  }
  implicit val four: COL[THREE] => COL[FOUR] = (three:COL[THREE]) => {
    class T extends COL[FOUR]( when(three.col === "3", lit("is three")).otherwise("not three"))
    new T
  }
  //WANT THREE TO extend ONE with TWO in some context

  class THREE extends Dependency[ONE with TWO,THREE]

  class FOUR extends Dependency[THREE,FOUR]
  //class FIVE extends JOIN[THREE,THREE,ONE]


  //three -- dependency[one,three] --- dependency[one with two,three] ---
}