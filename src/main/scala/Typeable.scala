package Typical.core;



import Typical.core.Typeable.COL
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.{universe => ru}
import ru._


object Typeable {

  private def build[U <: COL[_]](implicit tag: TypeTag[U],tagu:ClassTag[U]): U = {
    classTag[U].runtimeClass.newInstance().asInstanceOf[U].toA//.asInstanceOf[U]
  }
  private def buildcol[U <: COL[_]](implicit tag: TypeTag[U],tagu:ClassTag[U]): COL[U] = {
    classTag[U].runtimeClass.newInstance().asInstanceOf[U].toCOLL
  }


  abstract class COL[+A <: COL[_]](coldef: Column)(implicit taga: ClassTag[A]) extends Column(classTag[A].runtimeClass.getSimpleName()) {
    val col = this.coldef

    private def get[U >: A <: COL[U]](implicit tag: TypeTag[U],ctag:ClassTag[U]): U = {
      build[U].asInstanceOf[U]
    }
//  private def get[U <: COL[U]](implicit tag: TypeTag[U],ctag:ClassTag[U],ev:A<:< U): U = {
//    build[U].asInstanceOf[U]
//  }
    private[core] def getcol[U >: A <: COL[U]](implicit tag: TypeTag[U],ctag:ClassTag[U]): Column = get[U].col

  }
  def data[B<:COL[_]](implicit src:B,tagb:TypeTag[B],ctagb:ClassTag[B]) = {
    class T extends VALIDATED[B](build[B])
    new T
  }
  private[core] abstract class VALIDATED[B<:COL[_]](b:COL[B])(implicit src:B){

    def getcol[U>:B<:COL[U]](implicit tag: TypeTag[U],ctag:ClassTag[U]) = b.getcol[U]
  }

  implicit class Converter[A<:COL[_]](g: A => Column)(implicit taga:TypeTag[A],ctaga:ClassTag[A]) {
    def satisfy[B<:COL[_]]()(implicit tagb:ClassTag[B]): COL[A] => COL[B] = {
      (c:COL[A]) => {
        val a = build[A]
        class T extends COL[B](g(a)); new T
      }
    }
  }

  abstract class Dependency[-A<:COL[_],+B<:Dependency[_,B] with COL[B]](implicit ev: COL[A] => COL[B],taga:TypeTag[A],ctaga:ClassTag[A],tagb: ClassTag[B]) extends COL[B](coldef = ev(build[A]).col){
    val f = this.ev
  }

  implicit class DAT[+A <: DAT[_]](datadef: DataFrame)(implicit startdf: DataFrame, taga: ClassTag[A]) {
    val df = if (datadef == null) this.startdf else this.datadef
    def toDAT() = this
  }

  abstract class AXIOM[+A <: AXIOM[A]](implicit initdf: DataFrame, taga: ClassTag[A]) extends COL[A](initdf.col(classTag[A].runtimeClass.getSimpleName()))

  abstract class COLL[+A <: COL[_]](implicit taga:ClassTag[A]) extends COL[A](classTag[A].runtimeClass.newInstance().asInstanceOf[COL[A]].col)

  implicit class COLTOCOLL[A<:COL[_]](c:A)(implicit taga:ClassTag[A]) extends COLL[A]{
    val toCOLL =this.asInstanceOf[COL[A]]
    val toA = this.c
  }
  //col types can only reference a start df
  implicit class COLTODAT[A <: COL[_]](c: COL[A ])(implicit taga:ClassTag[A],tag:ClassTag[COLTODAT[A]], initdf: DataFrame) extends DAT[COLTODAT[A]](
    initdf.withColumn(c.toString(), c.col)
  )(startdf = initdf, tag) {
    val iscol = true;
    val todf = this.df

    def add[T <: Dependency[A, T]]()(implicit tagt: ClassTag[T]): DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c //.get[COL[U]]
      this.df.withColumn(t.toString(), t.f(u).col)
    }
  }
  implicit class COLTOTHING[A<: AXIOM[A],B<:COL[_],C<:COL[A with B]](c:COL[C]){
    val testing = true
  }
  implicit class AXTODAT[A <: AXIOM[A]](c: AXIOM[A])(implicit tag: ClassTag[A], startdf: DataFrame) extends DAT[AXTODAT[A]](null) {
    val isax = true;

    def add[T <: Dependency[A, T]]()(implicit tagt: ClassTag[T]): DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c //.get[COL[U]]
      this.df.withColumn(t.toString(), t.f(u).col)
    }
  }

  //dependent types only reference dependencies dataframe
  implicit class DEPTODAT[B <: COL[_], A <: Dependency[B, A]](dep: Dependency[B,A])(implicit taga: ClassTag[A], tagb: ClassTag[B], ttagb: TypeTag[B], startdf: DataFrame) extends DAT[DEPTODAT[B, A]](
    datadef = buildcol[B].df.withColumn(dep.toString(),dep.col)//.add[A]
  ) {
    val isDep = true;
    val df2 = (classTag[B]).runtimeClass.newInstance().asInstanceOf[COL[B]].add[A]//wihtcol
  }

  implicit class lmao[A<:AXIOM[A],B<:COL[_],C<:Dependency[A with B,C]](c:COL[C]){
    val islmao = true
  }
  abstract class JOIN[+A <: Dependency[_,A], +B <: Dependency[_,B], C <: AXIOM[C]](implicit cInA: Dependency[COL[C],_<:COL[A]] <:< A  , cInB: B <:< Dependency[_,B] , taga: ClassTag[A], tagb: ClassTag[B], tagc: ClassTag[C], ttaga:TypeTag[A],ttagb:TypeTag[B],ttagc:TypeTag[C], startdf: DataFrame) extends DAT[JOIN[A, B, C]](datadef = {
    buildcol[A].todf.join(buildcol[B].todf, build[C].toString())
  })

}