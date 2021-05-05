package dfep 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import scala.collection.Map
import scala.util.Random
import scala.math.exp

object Partition {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    implicit val sc = spark.sparkContext
    val edgeFileName = args(0)
    val partFileName = args(1)
    val numParts = args(2).toInt
    val c = args(3).toDouble
    val coreRate = args(4).toDouble

    // val vertex2part = sc
    //   .textFile(partFileName)
    //   .map(line => {
    //     val data = line.trim().split(" ")
    //     (data(0).toLong, data(1).toInt)
    //   })

    val tempGraph = GraphLoader
      .edgeListFile(sc, edgeFileName, false, numParts)
      // .joinVertices(vertex2part) { case (id, _, partId) =>
      //   partId
      // }
      .mapVertices((id,attr) => Random.nextInt(numParts))
    // tempGraph.inDegrees.foreach(println)
    // println("==============")
    // tempGraph.outDegrees.foreach(println)
    println(
      "Num nodes:",
      tempGraph.numVertices,
      "Num edges:",
      tempGraph.numEdges
    )

    val maxSize = Array.fill[Double](numParts)(
      c * tempGraph.numVertices * (1.0 - coreRate) / (numParts - 1)
    )
    maxSize(0) = coreRate * tempGraph.numVertices

    var graph = tempGraph.outerJoinVertices(tempGraph.degrees){   case (id, partId , Some(deg)) =>
      (partId, deg)
    }
    var partSize = graph.vertices
      .map { case (id, attr) => (attr._1, 0) }
      .countByKey()
      .toMap
    println("PartSize:", partSize)

    var degSize = graph.vertices
      .map { case (id, attr) => attr }
      .combineByKey(
        (x: Int) => (x.toLong, 1L),
        (acc: (Long, Long), x: Int)  => (acc._1 + x.toLong, acc._2 + 1L),
        (acc1: (Long, Long), acc2: (Long, Long))  => (acc1._1 +acc2._1, acc1._2 + acc2._2)
      ).map( { case (k,v) => (k, v._1.toDouble/v._2)}).collect().toMap
    println("degSize:", degSize)

    var penalty = partSize.map(x => (x._1, x._2 / maxSize(x._1)))
    println("penalty:", penalty)

    var nCut = graph.triplets
      .filter(x => x.srcAttr._1 != x.dstAttr._1 & x.srcAttr._1 != 0 & x.dstAttr._1 != 0)
      .count()
    println("Num edge-cut real:", nCut)

    var nCut2 = graph.triplets
      .filter(x => x.srcAttr._1 != x.dstAttr._1)
      .count()
    println("Num edge-cut all:", nCut2)
    // var graph = tempGraph
    for (i <- 1 to args(5).toInt) {
      val edgeInfo = graph
        .aggregateMessages[(List[Int], (Double, Int))](
          triplet => {
            triplet.sendToDst((List[Int](triplet.srcAttr._1), (triplet.srcAttr._2, 1)))
            triplet.sendToSrc((List[Int](triplet.dstAttr._1), (triplet.dstAttr._2, 1)))
          },
          (a, b) => (a._1 ++ b._1, (a._2._1 +b._2._1, a._2._2+b._2._2))
        )
        .map(v => {
          val neighLabelFreq =
            v._2._1.groupBy(identity).mapValues(x => x.size.toDouble)
          val neighLabelScore = neighLabelFreq
            .map(x => (x._1, x._2 / neighLabelFreq.values.sum - penalty(x._1))) 
          (v._1, neighLabelScore)
          // (v._1, neighLabelScore + (0-> (neighLabelScore.getOrElse(0,0.0) + 1*(v._2._2._1 /(50*v._2._2._2)-1.0))))
        })
      // edgeInfo.foreach(println)
      val transition = graph
        .outerJoinVertices(edgeInfo) { case (id, oldPartId, Some(eInfo)) =>
          // val scoreDict = if (eInfo.contains(0)) 
          //   eInfo + (0 -> (eInfo(0).toDouble +2*(oldPartId._2 /50-1.0)))
          //   else eInfo
          val scoreDict = eInfo + (0 -> (eInfo.getOrElse(0,0.0).toDouble +2*(oldPartId._2 /100-1.0)))
            
          // val scoreDict = eInfo
          val maxElem = scoreDict.maxBy(_._2)
          // if (oldPartId._1 == 0)
          // // if (oldPartId._1 == 0 & oldPartId._2 > 100)
          //   oldPartId
          // else 
          // if (eInfo.getOrElse(oldPartId._1, 0.0) < maxElem._2 & maxElem._1 != 0)
          if (eInfo.getOrElse(oldPartId._1, 0.0) < maxElem._2)
            (oldPartId._1, maxElem._1)
          else
            (oldPartId._1, oldPartId._1)

        }
        .vertices
        .map { case (id, attr) => (attr, 0) }
        .countByKey()
      println("transition:", transition)
      graph = graph.outerJoinVertices(edgeInfo) {
        case (id, oldPartId, Some(eInfo)) =>
          // val scoreDict = if (eInfo.contains(0)) 
          //   eInfo + (0 -> (eInfo(0).toDouble +2*(oldPartId._2 /50-1.0)))
          //   else eInfo
          val scoreDict = eInfo + (0 -> (eInfo.getOrElse(0,0.0).toDouble +2*(oldPartId._2 /100-1.0)))
            
          // val scoreDict = eInfo
          val maxElem = scoreDict.maxBy(_._2)
          // if (oldPartId._1 == 0)
          // // if (oldPartId._1 == 0 & oldPartId._2 > 100)
          //   oldPartId
          // else 
          // if (eInfo.getOrElse(oldPartId._1, 0.0) < maxElem._2 & maxElem._1 != 0)
          if (eInfo.getOrElse(oldPartId._1, 0.0) < maxElem._2)
            (maxElem._1, oldPartId._2)
          else
            oldPartId          
      }
      partSize = graph.vertices
        .map { case (id, attr) => (attr._1, 0) }
        .countByKey()
        .toMap
      println("PartSize:", partSize)
      degSize = graph.vertices
        .map { case (id, attr) => attr }
        .combineByKey(
          (x: Int) => (x.toLong, 1L),
          (acc: (Long, Long), x: Int)  => (acc._1 + x.toLong, acc._2 + 1L),
          (acc1: (Long, Long), acc2: (Long, Long))  => (acc1._1 +acc2._1, acc1._2 + acc2._2)
        ).map( { case (k,v) => (k, v._1.toDouble/v._2)}).collect().toMap
      println("degSize:", degSize)
      penalty = partSize.map(x => (x._1, x._2 / maxSize(x._1)))
      println("penalty:", penalty)

      nCut = graph.triplets
        .filter(x => x.srcAttr._1 != x.dstAttr._1 & x.srcAttr._1 != 0 & x.dstAttr._1 != 0)
        .count()
      println("Num edge-cut real:", nCut)

      nCut2 = graph.triplets
        .filter(x => x.srcAttr._1 != x.dstAttr._1)
        .count()
      println("Num edge-cut all:", nCut2)
      println("=========================================")
    }
    graph.vertices.map{case (id,attr) => id.toString + " " + attr._1.toString}
    .coalesce(1)
    .saveAsTextFile(args(6))
    spark.stop()
  }
}
