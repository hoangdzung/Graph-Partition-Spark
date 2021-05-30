package dfep 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.Partitioner
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.PeriodicGraphCheckpointer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import scala.collection.Map
import scala.util.Random
import scala.math.exp

class EdgePartitioner(override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
    case s: Int => {
      s - 1
    }
  }
}

object OriSpinner {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName(s"${this.getClass.getSimpleName}")
    //   .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    implicit val sc = spark.sparkContext
    val edgeFileName = args(0)
    val numPartsRDD = args(1).toInt
    val numParts = args(2).toInt
    val c = args(3).toDouble
    val lambdaEdge = args(4).toDouble
    val convergenceThreshold = args(5).toDouble
    val maxStep = args(6).toInt
    // val partOutPath = args(7)

    val rawGraph = GraphLoader
      .edgeListFile(sc, edgeFileName, false, numPartsRDD)

    val numNodes = rawGraph.numVertices
    val maxSize = Array.fill[Double](numParts)(
      c * numNodes / numParts
    )

    var graph = rawGraph.mapVertices((id, attr) => {
      Random.nextInt(numParts)
    })
    graph.cache()

    var stop = false
    var step = 0
    var bestScore = 0.0
    while (step < maxStep & !stop) {
      val stime0 = System.currentTimeMillis

      val partSize = graph.vertices
        .map { case (id, attr) => (attr, 0) }
        .countByKey()
        .toMap
      println("PartSize:", partSize)

      val penalty = partSize.map(x => (x._1, x._2 / maxSize(x._1)))
      println("penalty:", penalty)

      val nCut2 = graph.triplets
        .filter(x => x.srcAttr != x.dstAttr)
        .count()
      println("Num edge-cut all:", nCut2)
      println(
        s"Step0 = ${(System.currentTimeMillis - stime0) / 1000.0}"
      )
      val stime1 = System.currentTimeMillis

      val edgeInfo = graph
        .aggregateMessages[Map[Int, Int]](
          triplet => {
            triplet.sendToDst(
              Map(triplet.srcAttr -> 1)
            )
            triplet.sendToSrc(
              Map(triplet.dstAttr -> 1)
            )
          },
          (a, b) => a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0)) }
        )
        .map(v => {
          val neighLabelFreq =
            v._2
          val neighLabelScore = neighLabelFreq
            .map(x =>
              (
                x._1,
                x._2 / neighLabelFreq.values.sum + lambdaEdge * (1 - penalty(
                  x._1
                ))
              )
            )
          (v._1, neighLabelScore)
          // (v._1, neighLabelScore + (0-> (neighLabelScore.getOrElse(0,0.0) + 1*(v._2._2._1 /(50*v._2._2._2)-1.0))))
        })
        .cache()

      val tempGraph = graph.outerJoinVertices(edgeInfo) {
        case (id, oldPartId, Some(eInfo)) =>
          val scoreDict = eInfo + (0 -> (eInfo
            .getOrElse(0, 0.0)
            .toDouble))
          val maxElem = scoreDict.maxBy(_._2)
          if (eInfo.getOrElse(oldPartId, 0.0) < maxElem._2)
            ((oldPartId, maxElem._1), maxElem._2)
          else
            ((oldPartId, oldPartId), maxElem._2)
      }
      tempGraph.cache()
      val demand = tempGraph.vertices
        .map(x => x._2._1)
        .filter(x => x._1 != x._2)
        .map(x => (x._2, 1))
        .countByKey()
        .toMap
      val migrationProbabilities =
        demand.map(x => (x._1, (maxSize(x._1) - partSize.getOrElse(x._1,0L)) / x._2))
      println("migrationProbabilities:",migrationProbabilities)
      println(
        s"Step1 = ${(System.currentTimeMillis - stime1) / 1000.0}"
      )
      val stime2 = System.currentTimeMillis

      graph = tempGraph.mapVertices((id, attr) => {
        if (attr._1._1 == attr._1._2)
          attr._1._2
        else {
          if (Random.nextDouble() < migrationProbabilities(attr._1._2))
            attr._1._2
          else
            attr._1._1
        }
      })
      graph.cache()

      val curScore = tempGraph.vertices.map(x => x._2._2).reduce(_ + _)
      val progress = Math.abs(1.0 - 1.0 * curScore / bestScore)
      println("progress:", progress)
      println(
        s"Step2 = ${(System.currentTimeMillis - stime2) / 1000.0}"
      )
      if (progress < convergenceThreshold) {
        stop = true
      } else {
        step = step + 1
        if (bestScore < curScore) {
          bestScore = curScore
        }
      }
      println("=========================================")
    }
    // graph.triplets.flatMap(x => {
    //   if (x.srcAttr._1 == 0 && x.dstAttr._1 ==0 )
    //     (1 to numParts).map(i => (i, x.srcId.toString +" " +x.dstId.toString))
    //   else if (x.srcAttr._1 != 0 && x.dstAttr._1 ==0)
    //     Array((x.srcAttr._1, x.srcId.toString +" " +x.dstId.toString))
    //   else if (x.srcAttr._1 == 0 && x.dstAttr._1 !=0)
    //     Array((x.dstAttr._1, x.srcId.toString +" " +x.dstId.toString))
    //   else if (x.srcAttr._1 == x.dstAttr._1 )
    //     Array((x.srcAttr._1, x.srcId.toString +" " +x.dstId.toString))
    //   else
    //     Array[(String, String)]()
    // })
    // .partitionBy(new EdgePartitioner(numParts))
    // .map(x=> x._2)
    //   .saveAsTextFile(partOutPath)

    // var graph = rawGraph.outerJoinVertices(rawGraph.degrees){   case (id, partId , Some(deg)) =>
    //   (partId, deg)
    // }

    // val rawCoreGraph =
    //   graph.subgraph(vpred = (id, attr) => attr._1 == 0).connectedComponents()
    // rawCoreGraph.cache()
    // val largestCCIndex =
    //   rawCoreGraph.vertices.map(x => (x._2, 1)).countByKey().maxBy(_._2)._1
    // val coreGraph =
    //   rawCoreGraph.subgraph(vpred = (id, attr) => attr == largestCCIndex)
    // coreGraph.cache()
    // println("Reduce from "+rawCoreGraph.numVertices.toString + " nodes to "+ coreGraph.numVertices.toString + " nodes")
   
    // val partGraph = graph.outerJoinVertices(coreGraph.vertices) {
    //   (id, oldAttr, outDegOpt) =>
    //     outDegOpt match {
    //       case Some(outDeg) => 0
    //       case None         => {
    //         if (oldAttr._1 == 0)
    //           1+ Random.nextInt(numParts)
    //         else 
    //           oldAttr._1
    //       }
    //     }
    // }
    // partGraph
    //   .vertices
    //   .map{case (id, attr) => id.toString + " " + attr.toString}
    //   // .coalesce(1)
    //   .saveAsTextFile(partOutPath)
    spark.stop()
  }
}
