import Typical.core.Typeable._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit, when}

object Test {

  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val rand = scala.util.Random
  //startdf should come into the picture after joins on data have been performed
  implicit val startdf = Seq((1,2),(2,3),(3,4)).toDF("ONE","TWO")
  //----match axiomatic columns to axiom types
  class ONE extends AXIOM[ONE]
  class TWO extends AXIOM[TWO]
  class D extends AXIOM[D]
  //----define dependencies
  //this is how you specify multiple dependencies for a column
  //by declaring the dependency of the compound type of all the dependencies
  //a dependency of A,B for column C would then be A with B
  class THREE extends Dependency[ONE with TWO,THREE]
  class FOUR extends Dependency[THREE,FOUR]
  class FIVE extends Dependency[THREE with FOUR,FIVE]

  //----define dependency column mappings

  //remove this to prevent THREE from compiling
  val otmap: ONE with TWO  => Column = (ot:ONE with TWO) => {
    implicit val src = ot //set a COL type as implicit src. For dependencies to be accurate this should be set to the function input
    //implicit val tar = (new D) //uncomment this to see how multiple src columns produce compile error
    val one = data.getcol[ONE]
    val two = data.getcol[TWO]
    //val f = (new FIVE).col //DevNote: make cols/dependencies only constructible in columns package
    //val d = data[D].getcol[D] //uncomment this to see invalid data access from src columns
    when(one.mod(2) === 0,two).otherwise(one)
  }
  //remove this to prevent FOUR from compiling
  val threemap: THREE => Column = (three:THREE) => {
    when(three.col === "3", lit("is three")).otherwise("not three")
  }
  //remove this to prevent FIVE from compiling
  val fourmap: FOUR => Column = (four:FOUR) => {
    when(four.col === "is three",1).otherwise(0)
  }

  //----set the column definition of the dependencies to their corresponding maps
  //assert that otmap defines column of FOUR
  implicit val three = otmap.satisfy[THREE] //makke satisfy only callable in axioms package
  //assert that threemap defines the column of FOUR
  implicit val four = threemap.satisfy[FOUR]
  //assert that fourmap defines the column of FIVE
  implicit val five = fourmap.satisfy[FIVE]
  //(new THREE).testing
  //class T extends COL[ONE with TWO](null)
// (new THREE).testing
//  (new T).retesting
  ///----Show dataframes
  //(new FIVE).todf.show
  //(new FOUR)

}