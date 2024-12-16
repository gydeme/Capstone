package ch.epfl.lts2.wikipedia

import java.io.File
import java.nio.file.Paths
import java.sql.Timestamp
import java.time._
import java.time.format.DateTimeFormatter
import breeze.linalg._
import breeze.stats._
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.neo4j.spark._
import org.rogach.scallop._

case class PageRowThreshold(page_id: Long, threshold: Double)
case class PageVisitThrGroup(page_id: Long, threshold: Double, visits: List[(Timestamp, Int)])
case class PageVisitGroup(page_id: Long, visits: List[(Timestamp, Int)])
case class PageVisitElapsedGroup(page_id: Long, visits: List[(Int, Double)])
//case class PageStatRow(page_id: Long, mean: Double, variance: Double)

class PeakFinderConfig(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val cfgFile = opt[String](name="config", required=true)
  val outputPath = opt[String](name="outputPath", required=true)
  val language = opt[String](required=true, name="language")
  val parquetPagecounts = opt[Boolean](default=Some(false), name="parquetPagecounts")
  val parquetPagecountsPath = opt[String](default=Some(""), name="parquetPagecountPath")
  verify()
}

class PeakFinder(parquetPageCount: Boolean, parquetPagecountPath: String, dbHost: String, dbPort: Int,
                 dbUsername: String, dbPassword: String, keySpace: String, tableVisits: String,
                 tableMeta: String, neo4jUrl: String, neo4jUser: String, neo4jPass: String,
                 neo4jDb: String, outputPath: String) extends Serializable {

  println(s"DEBUG: Configuring Spark with Neo4j URL: $neo4jUrl")

  lazy val sparkConfig: SparkConf = new SparkConf().setAppName("Wikipedia activity detector")
    .set("spark.cassandra.connection.host", dbHost)
    .set("spark.cassandra.connection.port", dbPort.toString)
    .set("spark.cassandra.auth.username", dbUsername)
    .set("spark.cassandra.auth.password", dbPassword)
    .set("neo4j.url", neo4jUrl)
    .set("neo4j.authentication.type", "basic")
    .set("neo4j.authentication.basic.username", neo4jUser)
    .set("neo4j.authentication.basic.password", neo4jPass)
    .set("neo4j.database", neo4jDb)

  lazy val session: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  def queryNeo4jWithBatch[T](nodelist: Seq[Long], queryTemplate: String, batchSize: Int = 100000)
                            (processBatch: DataFrame => Unit): Unit = {
    val batches = nodelist.sliding(batchSize, batchSize)
    batches.zipWithIndex.foreach { case (batch, batchIndex) =>
      val query = queryTemplate.replace("$ids", s"[${batch.mkString(",")}]")
      val start = System.nanoTime()
      println(s"DEBUG: Starting batch $batchIndex with ${batch.size} nodes.")

      try {
        val result = session.read
          .format("org.neo4j.spark.DataSource")
          .option("query", query)
          .load()

        processBatch(result) // Process DataFrame directly
      } catch {
        case e: Exception =>
          println(s"ERROR: Batch $batchIndex failed with error: ${e.getMessage}")
          throw e
      }

      val end = System.nanoTime()
      println(f"DEBUG: Batch $batchIndex processed in ${(end - start) / 1e6}%.2f ms")
    }
  }




  //class PeakFinder(parquetPageCount: Boolean, parquetPagecountPath: String, dbHost: String, dbPort: Int,
//                 dbUsername: String, dbPassword: String, keySpace: String, tableVisits: String,
//                 tableMeta: String, neo4jUrl: String, neo4jUser: String, neo4jPass: String,
//                 neo4jDb: String, outputPath: String) extends Serializable {
//
//  println(s"DEBUG: Configuring Spark with Neo4j URL: $neo4jUrl")
//
//  lazy val sparkConfig: SparkConf = new SparkConf().setAppName("Wikipedia activity detector")
//    .set("spark.cassandra.connection.host", dbHost)
//    .set("spark.cassandra.connection.port", dbPort.toString)
//    .set("spark.cassandra.auth.username", dbUsername)
//    .set("spark.cassandra.auth.password", dbPassword)
//    .set("neo4j.url", neo4jUrl)
//    .set("neo4j.authentication.type", "basic")
//    .set("neo4j.authentication.basic.username", neo4jUser)
//    .set("neo4j.authentication.basic.password", neo4jPass)
//    .set("neo4j.database", neo4jDb)
//
//  println("DEBUG: Spark Configuration:")
//  sparkConfig.getAll.foreach { case (k, v) => println(s"$k = $v") }
//
//
//  lazy val session: SparkSession = SparkSession.builder.config(sparkConfig).getOrCreate()

  val pageCountLoader = if (parquetPageCount) new PageCountStatsLoaderParquet(session, parquetPagecountPath)
  else new PageCountStatsLoaderCassandra(session, keySpace, tableVisits, tableMeta)

  // Remaining logic unchanged

  private def unextendTimeSeries(inputExtended:Dataset[PageVisitGroup], startDate:LocalDate):Dataset[PageVisitGroup] = {
    import session.implicits._
    val startTs = Timestamp.valueOf(startDate.atStartOfDay.minusNanos(1))
    inputExtended.map(p => PageVisitGroup(p.page_id, p.visits.filter(v => v._1.after(startTs))) ).cache()
  }

  /**
    * Computes similarity of two time-series
    * @param v1 Time-series of edge start
    * @param v2 Time-series of edge end
    * @param isFiltered Specifies if filtering is required (divides values by the number of spikes)
    * @param lambda Similarity threshold (discard pairs having a lower similarity)
    * @return Similarity measure
    */
  def compareTimeSeries(v1:(String, Option[List[(Timestamp, Int)]]), v2:(String, Option[List[(Timestamp, Int)]]),
                        startTime:LocalDateTime, totalHours:Int,
                        isFiltered: Boolean, lambda: Double = 0.5): Double = {


    val v1Visits = v1._2.getOrElse(List[(Timestamp, Int)]())
    val v2Visits = v2._2.getOrElse(List[(Timestamp, Int)]())
    if (v1Visits.isEmpty || v2Visits.isEmpty) 0.0
    else TimeSeriesUtils.compareTimeSeries(v1Visits, v2Visits, startTime, totalHours, isFiltered, lambda)
  }

  private def compareTimeSeriesPearsonUnsafe(v1: Array[Double], v2:Array[Double]): Double = {
    // remove daily variations in visits data
    val vds1 = TimeSeriesUtils.removeDailyVariations(v1)
    val vds2 = TimeSeriesUtils.removeDailyVariations(v2)
    val c = TimeSeriesUtils.pearsonCorrelation(vds1, vds2)
    Math.max(0.0, c) // clip to 0, negative correlation means no interest for us
  }

  def compareTimeSeriesPearson(v1:(String, Option[List[(Timestamp, Int)]]), v2:(String, Option[List[(Timestamp, Int)]]), startTime:LocalDateTime, totalHours:Int): Double =
  {
    val v1Safe = v1._2.getOrElse(List[(Timestamp, Int)]())
    val v2Safe = v2._2.getOrElse(List[(Timestamp, Int)]())
    val vd1 = TimeSeriesUtils.densifyVisitList(v1Safe, startTime, totalHours)
    val vd2 = TimeSeriesUtils.densifyVisitList(v2Safe, startTime, totalHours)
    if (v1Safe.isEmpty || v2Safe.isEmpty) 0.0
    else compareTimeSeriesPearsonUnsafe(vd1, vd2) * totalHours // use scaling to mimick behavior of compareTimeSeries
  }

  def mergeEdges(e: Iterable[Edge[Double]]): Double = e.map(_.attr).sum

  def getStatsThreshold(pageStats:Dataset[PageStatRow], burstRate:Double):Dataset[PageRowThreshold] = {
    import session.implicits._
    pageStats.map(p => PageRowThreshold(p.page_id, p.mean + burstRate*scala.math.sqrt(p.variance)))
  }


  def getStats(input: DataFrame, startDate: LocalDate, endDate: LocalDate): Dataset[PageStatRow] = {
    import session.implicits._

    input.createOrReplaceTempView("page_visits")

    val statsQuery =
      s"""
         |SELECT page_id,
         |       AVG(visits) AS mean,
         |       VARIANCE(visits) AS variance
         |FROM page_visits
         |WHERE visit_date BETWEEN '${startDate}' AND '${endDate}'
         |GROUP BY page_id
     """.stripMargin

    session.sql(statsQuery).as[PageStatRow]
  }

  def extractPeakActivity(
                           startDate: LocalDate,
                           endDate: LocalDate,
                           inputExtended: Dataset[PageVisitGroup],
                           startDateExtend: LocalDate,
                           burstRate: Double,
                           burstCount: Int
                         ): Dataset[Long] = {
    import session.implicits._

    // Convert inputExtended to DataFrame for compatibility with getStats
    val inputExtendedDF: DataFrame = inputExtended.toDF()

    // Compute statistics for the extended input dataset
    val pageStats: Dataset[PageStatRow] = getStats(inputExtendedDF, startDateExtend, endDate)

    // Determine threshold for activity bursts
    val pageThr: Dataset[PageRowThreshold] = getStatsThreshold(pageStats, burstRate)

    // Filter the extended input dataset to retain visits after the start date
    val input: Dataset[PageVisitGroup] = unextendTimeSeries(inputExtended, startDate)

    // Join the dataset with thresholds and filter based on burst count
    val inputGrp: Dataset[PageVisitThrGroup] = input
      .join(pageThr, "page_id")
      .as[PageVisitThrGroup]

    // Identify active pages based on the burst count
    inputGrp
      .map(p => (p, p.visits.count(v => v._2 > p.threshold)))
      .filter(k => k._2 > burstCount)
      .map(p => p._1.page_id)
      .distinct
  }




  def writeParquet(df:DataFrame, outputPath: String) =  {
    df.write.mode("overwrite").option("compression", "gzip").parquet(outputPath)
  }

  def extractPeakActivityZscore(startDate:LocalDate, endDate:LocalDate, inputExtended: Dataset[PageVisitGroup], startDateExtend:LocalDate,
                          lag: Int, threshold: Double, influence: Double, activityThreshold:Int, saveOutput:Boolean=false): Dataset[Long] = {
    import session.implicits._

    val startTime = startDateExtend.atStartOfDay
    val totalHours = TimeSeriesUtils.getPeriodHours(startDateExtend, endDate)
    val extensionHours = TimeSeriesUtils.getPeriodHours(startDateExtend, startDate.minusDays(1)) // do not remove first day of studied period

    val activePages = inputExtended.map(p => PageVisitElapsedGroup(p.page_id, p.visits.map(v => (Duration.between(startTime, v._1.toLocalDateTime).toHours.toInt, v._2.toDouble))))
                                  .map(p => (p.page_id, new VectorBuilder(p.visits.map(f => f._1).toArray, p.visits.map(f => f._2).toArray, p.visits.size, totalHours).toDenseVector.toArray))
                                  .map(p => (p._1, TimeSeriesUtils.removeDailyVariations(p._2)))
                                  .map(p => (p._1, TimeSeriesUtils.smoothedZScore(p._2, lag, threshold, influence)))
                                  .map(p => (p._1, p._2.drop(extensionHours).count(_ > 0)))// remove extension from time series and count active hours
                                  .filter(_._2 >= activityThreshold) // discard insufficiently active pages

    if (saveOutput)
      writeParquet(activePages.toDF, Paths.get(outputPath, "activePages.pqt").toString)
    activePages.map(_._1)
  }

// ####attempt to replicate original method
//def extractActiveSubGraph(activeNodes: Dataset[Long], includeCategories: Boolean): Graph[String, (String, Double)] = {
//  import session.implicits._
//  println(s"DEBUG: Starting extractActiveSubGraph with ${activeNodes.count()} active nodes.")
//  val methodStart = System.nanoTime()
//
//  val nodeList = activeNodes.collect().toSeq
//  println(s"DEBUG: Collected node list. Total nodes to process: ${nodeList.size}.")
//
//  // Queries
//  val graphQueryTemplate = if (includeCategories) {
//    """
//    MATCH (p1:Page)-[r]->(p2:Page)
//    WHERE p1.id IN $ids AND p2.id IN $ids
//    RETURN p1.id AS source, p2.id AS target, p1.title AS sourceTitle, p2.title AS targetTitle, type(r) AS relationship
//    """
//  } else {
//    """
//    MATCH (p1:Page)-[r]->(p2:Page)
//    WHERE NOT "Category" IN labels(p1) AND NOT "Category" IN labels(p2)
//    AND p1.id IN $ids AND p2.id IN $ids
//    RETURN p1.id AS source, p2.id AS target, p1.title AS sourceTitle, p2.title AS targetTitle, type(r) AS relationship
//    """
//  }
//
//  val graphQuery = graphQueryTemplate.replace("$ids", s"[${nodeList.mkString(",")}]")
//
//  println("DEBUG: Querying Neo4j for full graph.")
//  val graphStart = System.nanoTime()
//  val graphDF = session.read
//    .format("org.neo4j.spark.DataSource")
//    .option("query", graphQuery)
//    .load()
//  println(f"DEBUG: Full graph query completed in ${(System.nanoTime() - graphStart) / 1e6}%.2f ms.")
//
//  // Convert to RDDs for GraphX
//  val verticesRDD = graphDF.selectExpr("source AS id", "sourceTitle AS title")
//    .union(graphDF.selectExpr("target AS id", "targetTitle AS title"))
//    .distinct()
//    .rdd
//    .map(row => (row.getAs[Long]("id"), xml.Utility.escape(row.getAs[String]("title")))) // Escape titles for XML compatibility
//
//  val edgesRDD = graphDF.select("source", "target", "relationship")
//    .rdd
//    .map(row => Edge(
//      row.getAs[Long]("source"),
//      row.getAs[Long]("target"),
//      (row.getAs[String]("relationship"), 1.0) // Include relationship and default weight
//    ))
//
//  // Create GraphX graph
//  val graph = Graph(verticesRDD, edgesRDD)
//  println(f"DEBUG: Graph created with ${graph.vertices.count()} vertices and ${graph.edges.count()} edges.")
//
//  println(f"DEBUG: Total time for extractActiveSubGraph: ${(System.nanoTime() - methodStart) / 1e6}%.2f ms.")
//  graph
//}


//####attempt to remake old method using cypher
def extractActiveSubGraph(
                           activeNodes: Dataset[Long],
                           includeCategories: Boolean,
                           batchSize: Option[Int] = None,
                           randomSubsetSize: Option[Int] = None
                         ): Graph[String, Double] = {
  import session.implicits._
  println(s"DEBUG: Starting extractActiveSubGraph with ${activeNodes.count()} active nodes.")
  val methodStart = System.nanoTime()

  // Validate input: only one mode can be enabled at a time
  require(
    batchSize.isEmpty || randomSubsetSize.isEmpty,
    "Both batching and randomSubset cannot be enabled simultaneously."
  )

  // Collect the active node list
  val nodeList = if (randomSubsetSize.isDefined) {
    println(s"DEBUG: Using randomSubset mode with size ${randomSubsetSize.get}.")
    activeNodes.sample(false, randomSubsetSize.get.toDouble / activeNodes.count()).collect().toSeq
  } else {
    println("DEBUG: Collecting full node list.")
    activeNodes.collect().toSeq
  }

  println(s"DEBUG: Collected node list. Total nodes to process: ${nodeList.size}.")

  // Define Cypher queries
  val nodesQueryTemplate = if (includeCategories) {
    """
    MATCH (p:Page) WHERE p.id IN $ids
    RETURN p.id AS id, p.title AS title
    """
  } else {
    """
    MATCH (p:Page) WHERE NOT "Category" IN labels(p) AND p.id IN $ids
    RETURN p.id AS id, p.title AS title
    """
  }

  val edgesQueryTemplate = if (includeCategories) {
    """
    MATCH (p1:Page)-[r]->(p2:Page)
    WHERE p1.id IN $ids AND p2.id IN $ids
    RETURN p1.id AS source, p2.id AS target
    """
  } else {
    """
    MATCH (p1:Page)-[r]->(p2:Page)
    WHERE NOT "Category" IN labels(p1) AND NOT "Category" IN labels(p2)
    AND p1.id IN $ids AND p2.id IN $ids
    RETURN p1.id AS source, p2.id AS target
    """
  }

  // Paths for intermediate Parquet files
  val verticesPath = Paths.get(outputPath, "vertices.parquet").toString
  val edgesPath = Paths.get(outputPath, "edges.parquet").toString

  // Clean up any existing Parquet files
  new File(verticesPath).delete()
  new File(edgesPath).delete()

  // Helper function to process batches
  def processBatches(nodes: Seq[Long]): Unit = {
    queryNeo4jWithBatch(nodes, nodesQueryTemplate) { batch =>
      println(s"DEBUG: Processing a batch of vertices with size ${batch.count()}.")
      batch.select("id", "title")
        .repartition(4) // Adjust for parallelism
        .write.mode("append").parquet(verticesPath)
    }

    queryNeo4jWithBatch(nodes, edgesQueryTemplate) { batch =>
      println(s"DEBUG: Processing a batch of edges with size ${batch.count()}.")
      batch.select("source", "target")
        .repartition(4) // Adjust for parallelism
        .write.mode("append").parquet(edgesPath)
    }
  }

  // Batch processing or single batch
  if (batchSize.isDefined) {
    println(s"DEBUG: Using batching mode with batch size ${batchSize.get}.")
    val batches = nodeList.sliding(batchSize.get, batchSize.get).toSeq
    batches.zipWithIndex.foreach { case (batch, batchIndex) =>
      println(s"DEBUG: Processing batch $batchIndex with ${batch.size} nodes.")
      processBatches(batch)
    }
  } else {
    println("DEBUG: Processing all nodes in a single batch.")
    processBatches(nodeList)
  }

  // Load vertices from Parquet
  val verticesLoadStart = System.nanoTime()
  println(s"DEBUG: Loading vertices from Parquet at $verticesPath.")
  val verticesRDD = session.read.parquet(verticesPath)
    .rdd.map(row => (row.getAs[Long]("id"), row.getAs[String]("title")))
  println(f"DEBUG: Vertices loaded in ${(System.nanoTime() - verticesLoadStart) / 1e6}%.2f ms.")

  // Load edges from Parquet
  val edgesLoadStart = System.nanoTime()
  println(s"DEBUG: Loading edges from Parquet at $edgesPath.")
  val edgesRDD = session.read.parquet(edgesPath)
    .rdd.map(row => Edge(
      row.getAs[Long]("source"),
      row.getAs[Long]("target"),
      1.0 // Default edge weight
    ))
  println(f"DEBUG: Edges loaded in ${(System.nanoTime() - edgesLoadStart) / 1e6}%.2f ms.")

  // Create GraphX graph
  val graph = Graph(verticesRDD, edgesRDD)
    .mapVertices((_, title) => xml.Utility.escape(title)) // Escape XML special characters

  println(f"DEBUG: Graph creation completed in ${(System.nanoTime() - methodStart) / 1e6}%.2f ms.")
  graph
}








  def getVisitsTimeSeriesGroup(startDate:LocalDate, endDate:LocalDate, language:String):Dataset[PageVisitGroup] = pageCountLoader.getVisitsTimeSeriesGroup(startDate, endDate, language)




  def getActiveTimeSeries(timeSeries:Dataset[PageVisitGroup], activeNodes:Dataset[Long], startDate:LocalDate, totalHours:Int, dailyMinThreshold:Int):Dataset[(Long, List[(Timestamp, Int)])] = {
    import session.implicits._
    val input = unextendTimeSeries(timeSeries, startDate)

    val activeTimeSeries = input.join(activeNodes.toDF("page_id"), "page_id").as[PageVisitGroup]//.map(p => (p.page_id, p.visits))
    activeTimeSeries.map(p => (p, TimeSeriesUtils.densifyVisitList(p.visits, startDate.atStartOfDay, totalHours).grouped(24).map(_.sum).max))
                                           .filter(_._2 >= dailyMinThreshold)
                                           .map(p => (p._1.page_id, p._1.visits))
  }


}


  object PeakFinder {
    def main(args: Array[String]): Unit = {
      println("DEBUG: Starting PeakFinder application")
      val dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val cfgBase = new PeakFinderConfig(args)
      val cfgDefault = ConfigFactory.parseString(
        """
      cassandra.db.port=9042
      peakfinder.useTableStats=false
      peakfinder.activityZScore=false
      peakfinder.pearsonCorrelation=false
      peakfinder.zscore.saveOutput=false
      peakfinder.minEdgeWeight=1.0
      peakfinder.includeCategories=false
      """
      )
      val cfg = ConfigFactory.parseFile(new File(cfgBase.cfgFile())).withFallback(cfgDefault)

      val outputPath = cfgBase.outputPath()
      val pf = new PeakFinder(
        parquetPageCount = cfgBase.parquetPagecounts(),
        parquetPagecountPath = cfgBase.parquetPagecountsPath(),
        dbHost = cfg.getString("cassandra.db.host"),
        dbPort = cfg.getInt("cassandra.db.port"),
        dbUsername = cfg.getString("cassandra.db.username"),
        dbPassword = cfg.getString("cassandra.db.password"),
        keySpace = cfg.getString("cassandra.db.keyspace"),
        tableVisits = cfg.getString("cassandra.db.tableVisits"),
        tableMeta = cfg.getString("cassandra.db.tableMeta"),
        neo4jUrl = cfg.getString("neo4j.url"),
        neo4jUser = cfg.getString("neo4j.user"),
        neo4jPass = cfg.getString("neo4j.password"),
        neo4jDb = cfg.getString("neo4j.database"),
        outputPath = outputPath
      )

      println(s"DEBUG: Neo4j URL passed to PeakFinder: ${cfg.getString("neo4j.url")}")

      val startDate = LocalDate.parse(cfg.getString("peakfinder.startDate"))
      val endDate = LocalDate.parse(cfg.getString("peakfinder.endDate"))
      val activityZscore = cfg.getBoolean("peakfinder.activityZScore")
      val pearsonCorr = cfg.getBoolean("peakfinder.pearsonCorrelation")
      val includeCategories = cfg.getBoolean("peakfinder.includeCategories")

      if (startDate.isAfter(endDate))
        throw new IllegalArgumentException("Start date is after end date")

      // retrieve visits time series plus history of equal length
      val visitsExtend = Period.between(startDate, endDate).getDays
      val startDateExtend = startDate.minusDays(visitsExtend)

      implicit val timestampIntEncoder: Encoder[(Timestamp, Int)] = Encoders.tuple(Encoders.TIMESTAMP, Encoders.scalaInt)
      implicit val pageVisitGroupEncoder: Encoder[PageVisitGroup] = Encoders.product[PageVisitGroup]

      val extendedTimeSeries: Dataset[PageVisitGroup] = pf.getVisitsTimeSeriesGroup(startDateExtend, endDate, cfgBase.language())
        .as[PageVisitGroup]
        .cache()

      val totalHours = TimeSeriesUtils.getPeriodHours(startDate, endDate)
      val startTime = startDate.atStartOfDay

      val activePages = if (!activityZscore)
        pf.extractPeakActivity(startDate, endDate,
          extendedTimeSeries, startDateExtend,
          burstRate = cfg.getDouble("peakfinder.burstRate"),
          burstCount = cfg.getInt("peakfinder.burstCount"))
      else
        pf.extractPeakActivityZscore(startDate, endDate, extendedTimeSeries, startDateExtend,
          lag = cfg.getInt("peakfinder.zscore.lag"),
          threshold = cfg.getDouble("peakfinder.zscore.threshold"),
          influence = cfg.getDouble("peakfinder.zscore.influence"),
          activityThreshold = cfg.getInt("peakfinder.zscore.activityThreshold"),
          saveOutput = cfg.getBoolean("peakfinder.zscore.saveOutput"))

      val activeTimeSeries = pf.getActiveTimeSeries(extendedTimeSeries, activePages, startDate,
        totalHours = totalHours,
        dailyMinThreshold = cfg.getInt("peakfinder.dailyMinThreshold")) //.cache()

//      val activePagesGraph = GraphUtils.toUndirected(
//            pf.extractActiveSubGraph(activePages, includeCategories = true, randomSubsetSize = Some(5000))
//          .outerJoinVertices(activeTimeSeries.rdd) { (_, title, visits) => (title, visits) },
//        pf.mergeEdges
//      )
        val activePagesGraph = GraphUtils.toUndirected(
          pf.extractActiveSubGraph(activePages, includeCategories = true, batchSize=Some(5000))
            .outerJoinVertices(activeTimeSeries.rdd) { (_, title, visits) => (title, visits) },
          pf.mergeEdges
        )

//      val activePagesGraph = GraphUtils.toUndirected(
//        pf.extractActiveSubGraph(activePages, includeCategories = true)
//          .outerJoinVertices(activeTimeSeries.rdd) { (_, title, visits) => (title, visits) },
//        pf.mergeEdges
//      )

      val trainedGraph = if (pearsonCorr) {
        activePagesGraph.mapTriplets(t => pf.compareTimeSeriesPearson(t.dstAttr, t.srcAttr, startTime, totalHours))
          .mapVertices((_, v) => v._1)
      } else {
        activePagesGraph.mapTriplets(t => pf.compareTimeSeries(t.dstAttr, t.srcAttr, startTime, totalHours,
            isFiltered = false, lambda = 0.5))
          .mapVertices((_, v) => v._1)
      }

      // Prune edges with weights below the threshold
      val prunedGraph = GraphUtils.removeLowWeightEdges(trainedGraph, minWeight = cfg.getDouble("peakfinder.minEdgeWeight"))

      // Remove singleton nodes
      val cleanGraph = GraphUtils.removeSingletons(prunedGraph)

      // Extract the largest connected component
      val finalGraph = GraphUtils.getLargestConnectedComponent(cleanGraph)


      if (outputPath.startsWith("hdfs://")) {
        val tmpPath = outputPath.replaceFirst("hdfs://", "")
        GraphUtils.saveGraphHdfs(finalGraph, weighted = true,
          fileName = Paths.get(tmpPath, "peaks_graph_" + dateFormatter.format(startDate) + "_" + dateFormatter.format(endDate) + ".gexf").toString)
      }
      else
        GraphUtils.saveGraph(finalGraph, weighted = true,
          fileName = Paths.get(outputPath, "peaks_graph_" + dateFormatter.format(startDate) + "_" + dateFormatter.format(endDate) + ".gexf").toString)

    }
}
