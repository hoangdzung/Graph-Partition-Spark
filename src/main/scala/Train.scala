// package dfep 
// import breeze.linalg._
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.graphx._
// import org.apache.spark.sql.SparkSession

// object GraphEmbedding {

//   def parseEmbeddings(embeddingsString: String): (Long, Array[Double]) = {

//     val split = embeddingsString.trim().split(" +")
//     (split(0).toLong, split.slice(1, split.size).map(e => e.toDouble))
//   }

// //   def aggregateEmbedding(
// //       matrix1: Map[Long, Array[Double]],
// //       matrix2: Map[Long, Array[Double]]
// //   ): Map[Long, Array[Double]] = {

// //     val commonNodes =
// //       embeddingDict1.keySet.intersect(embeddingDict2.keySet).toArray
// //     println(
// //       "aggregateEmbedding",
// //       embeddingDict1.size,
// //       embeddingDict2.size,
// //       commonNodes.size
// //     )

// //     if (commonNodes.size == 0)
// //       embeddingDict1 ++ embeddingDict2
// //     else {
// //       val matrix1 = DenseMatrix(commonNodes.map(x => embeddingDict1(x)): _*)
// //       val matrix2 = DenseMatrix(commonNodes.map(x => embeddingDict2(x)): _*)

// //       if (embeddingDict1.size > embeddingDict2.size) {
// //         val matrixX = inv(matrix2.t * matrix2) * matrix2.t * matrix1
// //         val keyArray = embeddingDict2.keySet.toArray
// //         val oldEmbeddingMatrix = DenseMatrix(
// //           keyArray.map(x => embeddingDict2(x)): _*
// //         )
// //         val newEmbeddingMatrix = (oldEmbeddingMatrix * matrixX).t
// //         val newEmbeddingDict2 = Array
// //           .range(0, keyArray.size)
// //           .map(x => (keyArray(x), newEmbeddingMatrix(::, x).toArray))
// //           .toMap
// //         newEmbeddingDict2 ++ embeddingDict1

// //       } else {
// //         val matrixX = inv(matrix1.t * matrix1) * matrix1.t * matrix2
// //         val keyArray = embeddingDict1.keySet.toArray
// //         val oldEmbeddingMatrix = DenseMatrix(
// //           keyArray.map(x => embeddingDict1(x)): _*
// //         )
// //         val newEmbeddingMatrix = (oldEmbeddingMatrix * matrixX).t
// //         val newEmbeddingDict1 = Array
// //           .range(0, keyArray.size)
// //           .map(x => (keyArray(x), newEmbeddingMatrix(::, x).toArray))
// //           .toMap
// //         newEmbeddingDict1 ++ embeddingDict2
// //       }
// //     }
// //   }


//   def main(args: Array[String]) {

//     val spark = SparkSession.builder
//       .appName(s"${this.getClass.getSimpleName}")
//       .config("spark.master", "local")
//       .getOrCreate()
//     implicit val sc = spark.sparkContext

//     val partDir = args(0)
//     val numParts = args(1).toInt

//     val edgeFileNames = sc.parallelize(0 to numParts-1, numParts).map(x => partDir+x.toString+".txt")
//     edgeFileNames.collect().foreach(println)

//     val pythonFile = args(2)
//     val startTime = System.currentTimeMillis
//     val embeddingsString = edgeFileNames
//       .pipe(pythonFile)

//     val embeddings = embeddingsString.map(parseEmbeddings)
//     embeddings.cache()
//     val coreNodes = embeddings.countByKey().filter(x => x._2 == numParts).map(x => x._1).toArray
//     val coreNodesBC = sc.broadcast(coreNodes)
//     // println("core nodes ", coreNodes )
//     val anchorIndex = embeddings.mapPartitionsWithIndex( (index, it) => List((index, it.size)).iterator).collect().maxBy(_._2)._1
//     val coreEmbeddings = embeddings.filter(x => coreNodesBC.value.contains(x._1)).glom()
//     .mapPartitionsWithIndex((index, it) => {
//         it.map ( x=> {
//             val embeddingsDict = x.toMap 
//             (index, DenseMatrix(coreNodes.map(i => embeddingsDict(i)): _*) )           
//         })

//     })
//     .collect() 
//     .toMap
//     val transformMatrixs = coreEmbeddings.map(x => {
//       val Y = coreEmbeddings(anchorIndex)
//       val W = inv(x._2.t * x._2) * x._2.t * Y
//       val b = Y - x._2 * W
//       (x._1, (W, b))
//     })
//     // println("transformMatrixs", transformMatrixs.size)
//     transformMatrixs.map(x => x._2._1.rows.toString + " " + x._2._1.cols.toString + ","+x._2._2.rows.toString + " " + x._2._2.cols.toString).foreach(println)
//     // val mergedEmbeddings = embeddings.mapPartitionsWithIndex( (index, it) => it.map(x => (index, x))).filter(x => !(x._1 != anchorIndex & coreNodesBC.value.contains(x._2._1)))
//     // .map(x => {
//     //     if (x._1 == anchorIndex)
//     //         x._2 
//     //     else {
//     //         val transformMatrix = transformMatrixs(x._1)
//     //         val oldEmbedding = DenseMatrix(
//     //             List(x._2._2): _*
//     //         )
//     //         val newEmbeddingMatrix = (oldEmbedding * transformMatrix._1 + transformMatrix._2).t
//     //         (x._2._1, newEmbeddingMatrix(::, 0).toArray)
//     //     }
//     // })
//     // .map(x => x._1.toString +" " + x._2.mkString(" "))

//     // mergedEmbeddings.saveAsTextFile(args(3))
//     println(
//       s"Training Elapsed = ${(System.currentTimeMillis - startTime) / 1000.0}"
//     )
   
//     spark.stop()
//   }
// }
