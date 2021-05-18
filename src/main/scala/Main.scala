// package dfep 

// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.Partitioner
// import org.apache.spark.graphx._
// import org.apache.spark.graphx.util.PeriodicGraphCheckpointer
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.SaveMode
// import scala.collection.Map
// import scala.util.Random
// import scala.math.exp

// class EdgePartitioner(override val numPartitions: Int) extends Partitioner {
//   def getPartition(key: Any): Int = key match {
//     case s: Int => {
//       s - 1
//     }
//   }
// }

// object Partition {

//   def main(args: Array[String]) {

//     val spark = SparkSession.builder
//       .appName(s"${this.getClass.getSimpleName}")
//     //   .config("spark.master", "local")
//       .getOrCreate()

//     import spark.implicits._

//     implicit val sc = spark.sparkContext
//     val edgeFileName = args(0)
//     val numPartsRDD = args(1).toInt
//     val numParts = args(2).toInt
//     val c = args(3).toDouble
//     val coreRate = args(4).toDouble
//     val scoreStrategy = args(5)
//     val lambdaEdge = args(6).toDouble
//     var lambdaRank = args(7).toDouble
//     val convergenceThreshold = args(8).toDouble
//     val maxStep = args(9).toInt
//     val coreOutPath = args(10)
//     val partOutPath = args(11)
//     // val vertex2part = sc
//     //   .textFile(partFileName)
//     //   .map(line => {
//     //     val data = line.trim().split(" ")
//     //     (data(0).toLong, data(1).toInt)
//     //   })

//     val rawGraph = GraphLoader
//       .edgeListFile(sc, edgeFileName, false, numPartsRDD)
//     // .joinVertices(vertex2part) { case (id, _, partId) =>
//     //   partId
//     // }
//     val numNodes = rawGraph.numVertices
//     val maxSize = Array.fill[Double](numParts + 1)(
//       c * numNodes * (1.0 - coreRate) / numParts
//     )
//     maxSize(0) = c * coreRate * numNodes

//     var scoreGraph: Graph[Double, Double] =
//       if (scoreStrategy == "pagerank")
//         rawGraph.pageRank(0.0001)
//       else if (scoreStrategy == "degree")
//         rawGraph
//           .outerJoinVertices(rawGraph.degrees) { case (id, _, Some(deg)) =>
//             1.0 * deg
//           }
//           .mapEdges(e => 1.0)
//       else 
//         rawGraph
//           .mapVertices((_,_) => Random.nextDouble())
//           .mapEdges(e => 1.0)        
//     val lowerBoundScores = scoreGraph.vertices
//       .map { case (id, attr) =>
//         attr
//       }
//       .top((3 * coreRate * numNodes).toInt)
//     val lowerBoundScore = lowerBoundScores(lowerBoundScores.size - 1)
//     println(lowerBoundScore)

//     var graph = scoreGraph.mapVertices((id, attr) => {
//       if (attr >= lowerBoundScore)
//         (0, attr)
//       else
//         (1 + Random.nextInt(numParts), attr)
//     })
//     graph.cache()
//     // graph.triplets.flatMap(x => {
//     //   if (x.srcAttr._1 == 0 && x.dstAttr._1 ==0 )
//     //     (1 to numParts).map(i => (i, x.srcId.toString +" " +x.dstId.toString))
//     //   else if (x.srcAttr._1 != 0 && x.dstAttr._1 ==0)
//     //     Array((x.srcAttr._1, x.srcId.toString +" " +x.dstId.toString))
//     //   else if (x.srcAttr._1 == 0 && x.dstAttr._1 !=0)
//     //     Array((x.dstAttr._1, x.srcId.toString +" " +x.dstId.toString))
//     //   else if (x.srcAttr._1 == x.dstAttr._1 )
//     //     Array((x.srcAttr._1, x.srcId.toString +" " +x.dstId.toString))
//     //   else
//     //     Array[(String, String)]()
//     // })
//     // .partitionBy(new EdgePartitioner(numParts))
//     //   .saveAsTextFile(partOutPath)

//     // var graph = rawGraph.outerJoinVertices(rawGraph.degrees){   case (id, partId , Some(deg)) =>
//     //   (partId, deg)
//     // }

//     var stop = false
//     var step = 0
//     var bestScore = 0.0
//     while (step < maxStep & !stop) {
//       val partSize = graph.vertices
//         .map { case (id, attr) => (attr._1, 0) }
//         .countByKey()
//         .toMap
//       println("PartSize:", partSize)

//       val scoreSize = graph.vertices
//         .map { case (id, attr) => attr }
//         .combineByKey(
//           (x: Double) => (x.toLong, 1L),
//           (acc: (Long, Long), x: Double) => (acc._1 + x.toLong, acc._2 + 1L),
//           (acc1: (Long, Long), acc2: (Long, Long)) =>
//             (acc1._1 + acc2._1, acc1._2 + acc2._2)
//         )
//         .map({ case (k, v) => (k, v._1.toDouble / v._2) })
//         .collect()
//         .toMap
//       println("scoreSize:", scoreSize)

//       val penalty = partSize.map(x => (x._1, x._2 / maxSize(x._1)))
//       println("penalty:", penalty)

//       val nCut = graph.triplets
//         .filter(x =>
//           x.srcAttr._1 != x.dstAttr._1 & x.srcAttr._1 != 0 & x.dstAttr._1 != 0
//         )
//         .count()
//       println("Num edge-cut real:", nCut)

//       val nCut2 = graph.triplets
//         .filter(x => x.srcAttr._1 != x.dstAttr._1)
//         .count()
//       println("Num edge-cut all:", nCut2)
//       val stime1 = System.currentTimeMillis

//       val edgeInfo = graph
//         .aggregateMessages[Map[Int, Int]](
//           triplet => {
//             triplet.sendToDst(
//               Map(triplet.srcAttr._1 -> 1)
//             )
//             triplet.sendToSrc(
//               Map(triplet.dstAttr._1 -> 1)
//             )
//           },
//           (a, b) => a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0)) }
//         )
//         .map(v => {
//           val neighLabelFreq =
//             v._2
//           val neighLabelScore = neighLabelFreq
//             .map(x =>
//               (
//                 x._1,
//                 x._2 / neighLabelFreq.values.sum + lambdaEdge * (1 - penalty(
//                   x._1
//                 ))
//               )
//             )
//           (v._1, neighLabelScore)
//           // (v._1, neighLabelScore + (0-> (neighLabelScore.getOrElse(0,0.0) + 1*(v._2._2._1 /(50*v._2._2._2)-1.0))))
//         })
//         .cache()

//       val tempGraph = graph.outerJoinVertices(edgeInfo) {
//         case (id, oldPartId, Some(eInfo)) =>
//           val scoreDict = eInfo + (0 -> (eInfo
//             .getOrElse(0, 0.0)
//             .toDouble + lambdaRank * (oldPartId._2 / lowerBoundScore - 1.0)))
//           val maxElem = scoreDict.maxBy(_._2)
//           if (eInfo.getOrElse(oldPartId._1, 0.0) < maxElem._2)
//             ((oldPartId._1, maxElem._1), oldPartId._2, maxElem._2)
//           else
//             ((oldPartId._1, oldPartId._1), oldPartId._2, maxElem._2)
//       }
//       tempGraph.cache()
//       val demand = tempGraph.vertices
//         .map(x => x._2._1)
//         .filter(x => x._1 != x._2)
//         .map(x => (x._2, 1))
//         .countByKey()
//         .toMap
//       val migrationProbabilities =
//         demand.map(x => (x._1, (maxSize(x._1) - partSize.getOrElse(x._1,0L)) / x._2))
//       println("migrationProbabilities:",migrationProbabilities)
//       println(
//         s"Step1 = ${(System.currentTimeMillis - stime1) / 1000.0}"
//       )
//       val stime2 = System.currentTimeMillis

//       graph = tempGraph.mapVertices((id, attr) => {
//         if (attr._1._1 == attr._1._2)
//           (attr._1._2, attr._2)
//         else {
//           if (Random.nextDouble() < migrationProbabilities(attr._1._2))
//             (attr._1._2, attr._2)
//           else
//             (attr._1._1, attr._2)
//         }
//       })
//       graph.cache()

//       val curScore = tempGraph.vertices.map(x => x._2._3).reduce(_ + _)
//       val progress = Math.abs(1.0 - 1.0 * curScore / bestScore)
//       println("progress:", progress)
//       println(
//         s"Step2 = ${(System.currentTimeMillis - stime2) / 1000.0}"
//       )
//       if (progress < convergenceThreshold) {
//         stop = true
//       } else {
//         step = step + 1
//         if (bestScore < curScore) {
//           bestScore = curScore
//         }
//       }
//       println("=========================================")
//       step = step + 1
//     }

//     val rawCoreGraph =
//       graph.subgraph(vpred = (id, attr) => attr._1 == 0).connectedComponents()
//     rawCoreGraph.cache()
//     val largestCCIndex =
//       rawCoreGraph.vertices.map(x => (x._2, 1)).countByKey().maxBy(_._2)._1
//     val coreGraph =
//       rawCoreGraph.subgraph(vpred = (id, attr) => attr == largestCCIndex)
//     coreGraph.cache()
//     println("Reduce from "+rawCoreGraph.numVertices.toString + " nodes to "+ coreGraph.numVertices.toString + " nodes")
//     coreGraph.vertices
//       .map { case (id, _) => id.toString + " 0" }
//       .coalesce(1)
//       .saveAsTextFile(coreOutPath)
//     val partGraph = graph.outerJoinVertices(coreGraph.vertices) {
//       (id, oldAttr, outDegOpt) =>
//         outDegOpt match {
//           case Some(outDeg) => 0
//           case None         => 1
//         }
//     }
//     partGraph
//       .subgraph(vpred = (id, attr) => attr == 1)
//       .edges
//       .map(e => e.srcId.toString + " " + e.dstId.toString)
//       // .coalesce(1)
//       .saveAsTextFile(partOutPath)
//     spark.stop()
//   }
// }
