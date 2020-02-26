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


  private[core] abstract class COL[+A <: COL[_]](coldef: Column)(implicit taga: ClassTag[A]) extends Column(classTag[A].runtimeClass.getSimpleName()) {
    val col = this.coldef

    private[core] def get[U >: A <: COL[U]](implicit tag: TypeTag[U],ctag:ClassTag[U]): U = {
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
    //def get[U>:B<:COL[U]](implicit tag: TypeTag[U],ctag:ClassTag[U]) = b.get[U]

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
    def COL[C<:COL[C]](implicit tag:TypeTag[C],ctag:ClassTag[C]) = {
      //cols.foldLeft(this.df)((a,c) => a.withColumn(c.toString(),c.col))
      this.df.withColumn(buildcol[C].toString(),buildcol[C].col)
    }
  }

  abstract class AXIOM[+A <: AXIOM[A]](implicit initdf: DataFrame, taga: ClassTag[A]) extends COL[A](initdf.col(classTag[A].runtimeClass.getSimpleName()))

  abstract class COLL[+A <: COL[_]](implicit taga:ClassTag[A],ttaga:TypeTag[A]) extends COL[A](classTag[A].runtimeClass.newInstance().asInstanceOf[COL[A]].col)

  implicit class COLTOCOLL[A<:COL[_]](c:A)(implicit taga:ClassTag[A],ttaga:TypeTag[A]) extends COLL[A]{
    val toCOLL =this.asInstanceOf[COL[A]]
    val toA = this.c
  }
  //col types can only reference a start df
  implicit class COLTODAT[A <: COL[_]](c: COL[A ])(implicit taga:ClassTag[A],ttaga:TypeTag[A],tag:ClassTag[COLTODAT[A]], initdf: DataFrame) extends DAT[COLTODAT[A]](
    initdf.withColumn(c.toString(), c.col)
  )(startdf = initdf, tag) {
    val iscol = true;
    val todf = this.df
    val columns = Seq(buildcol[A])
    def add[T <: Dependency[A, T]]()(implicit tagt: ClassTag[T]): DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c //.get[COL[U]]
      this.df.withColumn(t.toString(), t.f(u).col)
    }

  }
//  implicit class COLTOTHING[A<: AXIOM[A],B<:COL[_],X<:COL[A with B],C<:Dependency[X, C]](c:Dependency[X,C])(implicit taga:TypeTag[A],tagb:TypeTag[B],tagc:TypeTag[C],tagx:TypeTag[X]) extends DAT[COLTOTHING[A,B,X,C](datadef =
//    buildcol[C].df.
//  )(startdf={
//    val testing = true
//  }
//  implicit class COLTOOTHER[A<: AXIOM[A],B<:AXIOM[B],X<:COL[A with B],C<:COL[X]](c:COL[X])(implicit taga:TypeTag[A],tagb:TypeTag[B],tagc:TypeTag[C],tagx:TypeTag[X]){
//    val retesting = true;
//  }
  implicit class AXTODAT[A <: AXIOM[A]](c: AXIOM[A])(implicit tag: ClassTag[A],ttaga:TypeTag[A], startdf: DataFrame) extends DAT[AXTODAT[A]](startdf.select(build[A].toString)) {
    val isax = true;
    override val df = this.startdf
    def add[T <: Dependency[A, T]]()(implicit tagt: ClassTag[T]): DataFrame = {
      val t = classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      val u = c //.get[COL[U]]
      this.df.withColumn(t.toString(), t.f(u).col)
    }
//    override def COL[C<:COL[C]](implicit tag:TypeTag[C],ctag:ClassTag[C]) = {
//      //cols.foldLeft(this.df)((a,c) => a.withColumn(c.toString(),c.col))
//      buildcol[C].df.withColumn(c.toString(),c.col)
//    }
  }

  //dependent types only reference dependencies dataframe
//  implicit class DEPTODAT[B <: COL[_], A <: Dependency[B, A]](dep: Dependency[B,A])(implicit taga: ClassTag[A],ttaga:TypeTag[A], tagb: ClassTag[B], ttagb: TypeTag[B], startdf: DataFrame) extends DAT[DEPTODAT[B, A]](
//
//  )//(startdf = buildcol[B].df,tagdep)
//   {
//    val isDep = true;
//    val df2 = (classTag[B]).runtimeClass.newInstance().asInstanceOf[COL[B]].add[A]//wihtcol
//    val columns = Seq(buildcol[A]) :+ Seq(buildcol[B].columns:_*)
//  }

  implicit class lmao1[A<:AXIOM[A],B<:AXIOM[B]](c:A with B){
    val islmao = true
  }
  implicit class lmao2[A<:AXIOM[A],B<:COL[_],C<:Dependency[A with B,C]](c:COL[C]) {
    val islmao = true
  }
    abstract class JOIN[+A <: Dependency[_,A], +B <: Dependency[_,B], C <: AXIOM[C]](implicit cInA: Dependency[COL[C],_<:COL[A]] <:< A  , cInB: B <:< Dependency[_,B] , taga: ClassTag[A], tagb: ClassTag[B], tagc: ClassTag[C], ttaga:TypeTag[A],ttagb:TypeTag[B],ttagc:TypeTag[C], startdf: DataFrame) extends DAT[JOIN[A, B, C]](datadef = {
    buildcol[A].todf.join(buildcol[B].todf, build[C].toString())
  })

}