package core;


import org.apache.spark.sql.{Column,DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import scala.reflect.{ClassTag,classTag}
import scala.reflect.runtime.{universe => ru}
import ru._

abstract class COL[ +A <: COL[_] ](coldef:Column)(implicit taga:ClassTag[A]) extends Column(classTag[A].runtimeClass.getSimpleName()){
  val col = this.coldef
  //def get[U<:COL[_]](implicit tag: ClassTag[U]):U = classTag[U].runtimeClass.
  def get[U<:COL[_]](implicit tag: TypeTag[U]):U = {
    val m = ru.runtimeMirror(getClass().getClassLoader())
    val classu = ru.typeOf[U].typeSymbol.asClass
    val cm = m.reflectClass(classu)
    val ctor = ru.typeOf[U].decl(ru.termNames.CONSTRUCTOR).asMethod
    val ctorm = cm.reflectConstructor(ctor)
    ctorm().asInstanceOf[U]
  }
//  def include[U<:COL[U],T]:COL[T] = {
//    class Tt extends T with COL[T]
//    new Tt()
//  }
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
  implicit val ev: COL[C with Y] => COL[X] = ( cd: COL[C with Y]) => {
    val d = cd.get[Y]
    val c = cd.get[C]
    class NEW extends COL[X](when(d.col === "even",c.col).otherwise("no"))
    new NEW()
  }
  implicit val ev2: COL[D] => COL[Y] = (d:COL[D]) => {
    class NEW extends COL[Y](when(d.col.mod(2)===0,"even").otherwise("odd"))
    new NEW()
  }
  val rand = scala.util.Random
  class D extends COL[D](lit(rand.nextInt()))
  class C extends COL[C](lit("C"))
  class Y extends Dependency[D,Y]
  class X extends Dependency[C with Y, X]
  def run(): DataFrame = {
    val df = Seq(1,2,3,4,5).toDF
    val d = new D()
    val c = new C()
    val df2 = df.withColumn(d.toString(),d.col)
    val df3 = df.withColumn(c.toString(),c.col)
    val df4 = add[D,Y](df3)
    add[C with Y,X](df4)
  }

}