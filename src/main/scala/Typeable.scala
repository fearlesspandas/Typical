package Typical.core;


import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}
import ru._


object Typeable{
  abstract class COL[ +A <: COL[_] ](coldef:Column)(implicit taga:ClassTag[A]) extends Column(classTag[A].runtimeClass.getSimpleName()){
    val col = this.coldef
    def get[U>:A<:AXIOM[U]](implicit tag: TypeTag[U]):U = {
      val m = ru.runtimeMirror(getClass().getClassLoader())
      val classu = ru.typeOf[U].typeSymbol.asClass
      val cm = m.reflectClass(classu)
      val ctor = ru.typeOf[U].decl(ru.termNames.CONSTRUCTOR).asMethod
      val ctorm = cm.reflectConstructor(ctor)
      ctorm().asInstanceOf[U]
    }
  }
  implicit class thing[A<:COL[_]](a:A){
    val toThing = a.asInstanceOf[COL[A]]
  }
  implicit class Converter[A<:COL[_]](g: COL[A] => Column)  {
    def satisfy[B<:COL[_]]()(implicit tagb:ClassTag[B]): COL[A] => COL[B] = {
      (c:COL[A]) => {class T extends COL[B](g(c)); new T}
    }
  }
  abstract class Dependency[-A<:COL[_],+B<:Dependency[_,B] with COL[B]](implicit ev: COL[A] => COL[B],taga:ClassTag[A],tagb: ClassTag[B]) extends COL[B](coldef = ev(classTag[A].runtimeClass.newInstance().asInstanceOf[COL[A]]).col){
    val f = this.ev
  }
  implicit class DAT[ +A <: DAT[_] ](datadef:DataFrame)(implicit startdf:DataFrame,taga:ClassTag[A]){
    val df = if(datadef == null) this.startdf else this.datadef
  }
  abstract class AXIOM[+A<:AXIOM[A]](implicit initdf:DataFrame,taga:ClassTag[A]) extends COL[A](initdf.col(classTag[A].runtimeClass.getSimpleName()))
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
  implicit class AXTODAT[+A<:AXIOM[A]](c:AXIOM[A])(implicit tag: ClassTag[A],startdf:DataFrame) extends DAT[AXTODAT[A]](null){
    val iscol = true;
    def add[T<:Dependency[A,T]]()(implicit tagt:ClassTag[T]):DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c//.get[COL[U]]
      this.df.withColumn(t.toString(),t.f(u).col)
    }
  }
  //dependent types only reference dependencies dataframe
  implicit class DEPTODAT[B<:COL[_],A<:Dependency[B,A]](dep:A)(implicit taga: ClassTag[A],tagb:ClassTag[B],ttagb: TypeTag[B],startdf:DataFrame) extends DAT[DEPTODAT[B,A]](
    datadef= (classTag[B]).runtimeClass.newInstance().asInstanceOf[COL[B]].add[A]
  ){
    val isDep = true;
    override val df = (classTag[B]).runtimeClass.newInstance().asInstanceOf[COL[B]].add[A]
  }
}
