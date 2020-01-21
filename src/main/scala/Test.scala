package core
import Typical.core.Typeable._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit, when}

object Test {

  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val rand = scala.util.Random
  //startdf should come into the picture after joins on data have been performed
  implicit val startdf = Seq((1,2),(2,3),(3,4)).toDF("ONE","TWO")

  class ONE extends AXIOM[ONE]
  class TWO extends AXIOM[TWO]
  class D extends COL[ONE](null)

val otmap: ONE with TWO with D => Column = (ot:ONE with TWO) => {
  val one = ot.getcol[ONE]
  val two = ot.getcol[TWO]
  when(one.mod(2) === 0,two).otherwise(one)
}
  val threemap: THREE => Column = (three:THREE) => {
    when(three.col === "3", lit("is three")).otherwise("not three")
  }
  val fourmap: FOUR => Column = (four:FOUR) => {
    when(four.col === "is three",1).otherwise(0)
  }
  implicit val three = otmap.satisfy[THREE]
  implicit val four = threemap.satisfy[FOUR]
  implicit val five = fourmap.satisfy[FIVE]
  class THREE extends Dependency[ONE with TWO with D,THREE]

  class FOUR extends Dependency[THREE,FOUR]
  class FIVE extends Dependency[THREE with FOUR,FIVE]
  //class FIVE extends JOIN[THREE,THREE,ONE]
  //class T extends COL[ONE with TWO](null)

 // (new THREE).islmao
  (new FIVE).isDep
  //(new T).iscol
  //three <: Dependency[ONE with TWO,THREE]
  //three <: Dependency[ONE,THREE]
  //one with two <: one
  //B<: Axiom --
  //A <: B
  //C<:Dependency[B,C] --
  //(new THREE).islmao
  //three -- dependency[one,three] --- dependency[one with two,three] ---
}