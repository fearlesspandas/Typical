package core;


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}
import ru._
import scala.annotation.implicitNotFound
trait biggerthandep
 abstract class COL[ +A <: COL[_] ](coldef:Column)(implicit taga:ClassTag[A]) extends Column(classTag[A].runtimeClass.getSimpleName()) with biggerthandep{
  val col = this.coldef
  def get[U>:A<:COL[_]](implicit tag: TypeTag[U]):U = {
    val m = ru.runtimeMirror(getClass().getClassLoader())
    val classu = ru.typeOf[U].typeSymbol.asClass
    val cm = m.reflectClass(classu)
    val ctor = ru.typeOf[U].decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctor)
    ctorm().asInstanceOf[U]
  }
}


abstract class Dependency[-A<:COL[_],+B<:Dependency[_,B] with COL[B]](implicit ev: COL[A] => COL[B],taga:ClassTag[A],tagb: ClassTag[B]) extends COL[B](coldef = ev(classTag[A].runtimeClass.newInstance().asInstanceOf[COL[A]]).col){
  val f = this.ev

}

object Dependency{

  implicit class DAT[ +A <: DAT[_] ](datadef:DataFrame)(implicit startdf:DataFrame,taga:ClassTag[A]){
    val df = if(datadef == null) this.startdf else this.datadef
//    val defaultdf = startdf
//    val df = if(dfdef == defaultdf) defaultdf else dfdef
  }


//  def add[U<:COL[_],T<:Dependency[U,T]](dataFrame: DataFrame)(implicit tagt:ClassTag[T],tagu:TypeTag[U],classtagu:ClassTag[U]):DataFrame = {
//    val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
//    val u = classTag[U].runtimeClass.newInstance().asInstanceOf[COL[U]]//.get[COL[U]]
//    dataFrame.withColumn(t.toString(),t.f(u).col)
//  }
//  def add[U<:COL[_],T<:Dependency[U,T]](dataFrame: DataFrame,dep:Dependency[U,T])(implicit tagt:ClassTag[T],tagu:TypeTag[U],classtagu:ClassTag[U]):DataFrame = {
//    val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
//    val u = classTag[U].runtimeClass.newInstance().asInstanceOf[COL[U]]//.get[COL[U]]
//    dataFrame.withColumn(t.toString(),t.f(u).col)
//  }
//  def addType[U<:COL[_],T<:Dependency[U,T]](dataFrame: DataFrame,dep:Dependency[U,T])(implicit tagt:ClassTag[T],tagu:TypeTag[U],classtagu:ClassTag[U]):U with T = {
//    val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
//    val u = classTag[U].runtimeClass.newInstance().asInstanceOf[COL[U]]//.get[COL[U]]
//    dataFrame.withColumn(t.toString(),t.f(u).col)
//
//  }


}
object Test {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  import Dependency._
  class D extends COL[D](lit (null))
  val df = Seq((0 to 10000):_*).toDF("D")
  val rand = scala.util.Random

  implicit val startdf = Seq((1,2),(2,3),(3,4)).toDF("ONE","TWO")
  abstract class AXIOM[A<:AXIOM[_]](implicit initdf:DataFrame,taga:ClassTag[A]) extends COL[A](initdf.col(classTag[A].runtimeClass.getSimpleName()))
  class ONE extends AXIOM[ONE]//(startdf.col("ONE"))
  class TWO extends AXIOM[TWO]//(startdf.col("TWO"))
  //col types can only reference a start df
  implicit class COLTODAT[+A<:COL[_]](c:COL[A])(implicit tag: ClassTag[COLTODAT[A]],initdf:DataFrame) extends DAT[COLTODAT[A]](
    initdf.withColumn(c.toString(),c.col)
  )(startdf=initdf,tag)
  {
    val iscol = true;
    def add[T<:Dependency[A,T]]()(implicit tagt:ClassTag[T]):DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c//.get[COL[U]]
      this.df.withColumn(t.toString(),t.f(u).col)
    }
  }
  implicit class AXTODAT[+A<:AXIOM[_]](c:AXIOM[A])(implicit tag: ClassTag[A]) extends DAT[AXTODAT[A]](null){
    val iscol = true;
    def add[T<:Dependency[A,T]]()(implicit tagt:ClassTag[T]):DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c//.get[COL[U]]
      this.df.withColumn(t.toString(),t.f(u).col)
    }
  }
  //dependent types only reference dependencies
  implicit class DEPTODAT[B<:COL[_],A<:Dependency[B,A]](dep:A)(implicit taga: ClassTag[A],tagb:ClassTag[B],ttagb: TypeTag[B]) extends DAT[DEPTODAT[B,A]](
    datadef= (classTag[B]).runtimeClass.newInstance().asInstanceOf[COL[B]].add[A]
  ){
    val isDep = true;
    override val df = (classTag[B]).runtimeClass.newInstance().asInstanceOf[COL[B]].add[A]
  }
  val newone = new ONE()

  implicit val three: COL[ONE with TWO ] => COL[THREE] = (ot:COL[ONE with TWO]) => {
    val one = ot.get[ONE]
    val two = ot.get[TWO]
    class T extends COL[THREE](when(one.col.mod(2) === 0,two.col).otherwise(one.col))
    new T
  }
  implicit val four: COL[THREE] => COL[FOUR] = (three:COL[THREE]) => {
    class T extends COL[FOUR]( when(three.col === "3", lit("is three")).otherwise("not three"))
    new T
  }
  //implicit class Col(thing: ONE with TWO) extends COL[ONE with TWO](lit(null))
  //class T[+A<:COL[A],+B<:COL[B]] extends A with B
  class THREE extends Dependency[ONE with TWO,THREE]
  class FOUR extends Dependency[THREE,FOUR]
  //(new THREE).add[FOUR]
  //val retdf = add(startdf,new THREE)
  //val testdf = (new THREE).df
  //val fourdf = (new FOUR).df
  //alternative method of specifying dependencies
  //implicit class FOUR(thing: ONE with TWO) extends COL[FOUR](when(thing.get[ONE].col === "",""))  {val four = this;}
  //val t = new T[ONE,TWO].asInstanceOf[ONE with TWO]
  //add[ONE with TWO,FOUR](startdf)


}