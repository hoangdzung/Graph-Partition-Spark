package dfep 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import scala.collection.Map
import scala.util.Random
import scala.math.exp
import scala.util.Try

object Convert {
  def tryToDouble( s: String ) = Try(s.toDouble).toOption
  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName(s"${this.getClass.getSimpleName}")
      // .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    implicit val sc = spark.sparkContext
    val edgeFileName = args(0)
    val numPartsRDD = args(1).toInt
    val outPath = args(2)

    val edges = sc.textFile(edgeFileName, numPartsRDD)
    .map(line => line.trim.split(" +"))
    .filter(x=> {
      (x.size == 2) && (tryToDouble(x(0)) != None) && (tryToDouble(x(1)) != None)
    })
    .map(x => x.map(i => i.toDouble.toLong.toString).mkString(" "))
 
    edges.saveAsTextFile(outPath)
    spark.stop()
  }
}
