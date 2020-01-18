package core;


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}
import ru._
import scala.annotation.implicitNotFound

 abstract class COL[ +A <: COL[_] ](coldef:Column)(implicit taga:ClassTag[A]) extends Column(classTag[A].runtimeClass.getSimpleName()){
  val col = this.coldef
  def get[U>:A<:COL[_]](implicit tag: TypeTag[U]):U = {
    val m = ru.runtimeMirror(getClass().getClassLoader())
    val classu = ru.typeOf[U].typeSymbol.asClass
    val cm = m.reflectClass(classu)
    val ctor = ru.typeOf[U].decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctor)
    ctorm().asInstanceOf[U]
  }
//   def get2[T>:A<:COL[_]](implicit ttag:TypeTag[T]) = {
//     def f[Z<:COL[_]](implicit tag:TypeTag[Z]) = {this.get[Z,T]}
//     f[T]
//   }
//   def get[U>:A](implicit tag: TypeTag[U]):U = {
//     val m = ru.runtimeMirror(getClass().getClassLoader())
//     val classu = ru.typeOf[U].typeSymbol.asClass
//     val cm = m.reflectClass(classu)
//     val ctor = ru.typeOf[U].decl(ru.termNames.CONSTRUCTOR).asMethod
//     val ctorm = cm.reflectConstructor(ctor)
//     ctorm().asInstanceOf[U]
//   }
}

abstract class Dependency[A<:COL[_],B<:Dependency[_,B] with COL[B]](implicit ev: COL[A] => COL[B],taga:ClassTag[A],tagb: ClassTag[B]) extends COL[B](coldef = ev(classTag[A].runtimeClass.newInstance().asInstanceOf[COL[A]])){
  val f = this.ev

}

object Dependency{
  def add[U<:COL[_],T<:Dependency[U,T]](dataFrame: DataFrame)(implicit tagt:ClassTag[T],tagu:TypeTag[U],classtagu:ClassTag[U]):DataFrame = {
    val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
    val u = classTag[U].runtimeClass.newInstance().asInstanceOf[COL[U]]//.get[COL[U]]
    dataFrame.withColumn(t.toString(),t.f(u).col)
  }
}
object Test {
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  import Dependency._
  class D extends COL[D](lit (null))
  val df = Seq((0 to 10000):_*).toDF("D")
  val rand = scala.util.Random

  val startdf = Seq((1,2),(2,3),(3,4)).toDF("ONE","TWO")
  class ONE extends COL[ONE](startdf.col("ONE"))
  class TWO extends COL[TWO](startdf.col("TWO"))
  implicit val three: COL[ONE with TWO ] => COL[THREE] = (ot:COL[ONE with TWO]) => {
    val one = ot.get[ONE]
    val two = ot.get[TWO]
    class T extends COL[THREE](when(one.col.mod(2) === 0,two.col).otherwise(one.col))
    new T
  }
  class THREE extends Dependency[ONE with TWO,THREE]
  val retdf = add[ONE with TWO,THREE](startdf)


//  def run(): DataFrame = {
//
//    val d = new D()
//    val c = new C()
//    //val df2 = df.withColumn(d.toString(),d.col)
//    val df3 = df.withColumn(c.toString(),c.col)
//    val df4 = add[D,Y](df3)
//
//    return df4
//    //add[C with Y,X](df4)
//  }

}