package movieRecommend

import java.io.File

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{PAlgorithm, Params}
import org.apache.predictionio.data.storage.BiMap
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.{ALS, Rating => MLlibRating}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class ALSAlgorithmParams(
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]) extends Params

class ALSAlgorithm(val ap: ALSAlgorithmParams)
  extends PAlgorithm[PreparedData, ALSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]
  @transient lazy val config = ConfigFactory.parseFile(new File("mysql.conf"))
  private lazy val mysqlConfig = config.getConfig("mysql")
  private lazy val host = mysqlConfig.getString("host")
  private lazy val user = mysqlConfig.getString("user")
  private lazy val password = mysqlConfig.getString("password")


  if (ap.numIterations > 30) {
    logger.warn(
      s"ALSAlgorithmParams.numIterations > 30, current: ${ap.numIterations}. " +
      s"There is a chance of running to StackOverflowException." +
      s"To remedy it, set lower numIterations or checkpoint parameters.")
  }

  override
  def train(sc: SparkContext, data: PreparedData): ALSModel = {
    // MLLib ALS cannot handle empty training data.
    require(!data.ratings.take(1).isEmpty,
      s"RDD[Rating] in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preparator generates PreparedData correctly.")
    // Convert user and item String IDs to Int index for MLlib

    val userStringIntMap = BiMap.stringInt(data.ratings.map(_.user))
    val itemStringIntMap = BiMap.stringInt(data.ratings.map(_.item))
    val mllibRatings: RDD[MLlibRating] = data.ratings.map(r =>
      // MLlibRating requires integer index for user and item
      MLlibRating(userStringIntMap(r.user), itemStringIntMap(r.item), r.rating)
    )
    logger.info("***************************************data num: " + mllibRatings.count())

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // Set checkpoint directory
    // sc.setCheckpointDir("checkpoint")

    // If you only have one type of implicit event (Eg. "view" event only),
    // set implicitPrefs to true
    val implicitPrefs = false
    val als = new ALS()
    als.setUserBlocks(-1)
    als.setProductBlocks(-1)
    als.setRank(ap.rank)
    als.setIterations(ap.numIterations)
    als.setLambda(ap.lambda)
    als.setImplicitPrefs(implicitPrefs)
    als.setAlpha(1.0)
    als.setSeed(seed)
    als.setCheckpointInterval(10)
    val m = als.run(mllibRatings)

    //按照平均评分高低对影片进行排序
    val movieSortByRate: Array[(String, Double)] = data.ratings.map(k => (k.item, k.rating)).groupByKey().map(k => (k._1, k._2.sum/k._2.size)).collect().sortWith(_._2>_._2)

    new ALSModel(
      rank = m.rank,
      userFeatures = m.userFeatures,
      productFeatures = m.productFeatures,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      movieSortByRate = movieSortByRate,
      productFeaturesMap = m.productFeatures.collectAsMap.toMap
    )

  }

  import java.sql.{Connection, DriverManager}

  def getUserRateHistory(user:String) ={
    Class.forName("com.mysql.jdbc.Driver")
    logger.info("mysql驱动加载成功")
    val mysqlConnect = DriverManager.getConnection(this.host,  this.user , "YS.123456" )
    val sql = "select film_id from t_bus_raty where id like " + "\"" + user + "\""
    val queryResult = mysqlConnect.createStatement().executeQuery(sql)
    val history = ArrayBuffer[String]()
    while(queryResult.next()){
      history.+=:(queryResult.getNString(1))
    }
    history.toSet
  }

  private
  def getTopN[T](s: Seq[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = mutable.PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }
    q.dequeueAll.toSeq.reverse
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  def getHistorySimilar(queryFeatures:Vector[Array[Double]], productFeatures: Map[Int, Array[Double]]) = {
      val indexScores: Array[(Int, Double)] = if (queryFeatures.isEmpty) {
      logger.info(s"No productFeatures vector for history items.")
      Array[(Int, Double)]()
    } else {
      productFeatures.par // convert to parallel collection
        .mapValues { f =>
          queryFeatures.map{ qf =>
            cosine(qf, f)
          }.sum
        }
        .filter(_._2 > 0) // keep items with score > 0
        .seq // convert back to sequential collection
        .toArray
    }
    indexScores
  }

  override
  def predict(model: ALSModel, query: Query): PredictedResult = {
    if(query.item.getOrElse("none").equals("none")){
      val user = query.user.getOrElse("none")
      //查询用户评分过movie
      val history: Set[String] = getUserRateHistory(user)
      val ord = Ordering.by[(String, Double), Double](_._2).reverse
      val itemIntStringMap: BiMap[Int, String] = model.itemStringIntMap.inverse
      // Convert String ID to Int index for Mllib
      val als: Array[(String, Double)] = model.userStringIntMap.get(user).map { userInt =>
        val itemScores: Array[(String, Double)] = model.recommendProducts(userInt, 3 * query.num)
          .map (r => (itemIntStringMap(r.product), r.rating))
          .filter(k => !history.contains(k._1)).take(query.num)//过滤已经评分过
        itemScores
      }.getOrElse(
        Array.empty
      )
      logger.info("***************************************als num: " + als.length)
      //判断als结果个数，如果小于query指定num，则使用用户已评分过movie的相似movie进行补充
      if(als.length < query.num){
        if (history.nonEmpty){
          //将用户评分记录转变为vector
          val queryFeatures: Vector[Array[Double]] = history.toVector.map(k => model.itemStringIntMap.getOrElse(k, -10000))
            // productFeatures may not contain the requested item
            .map { item =>  model.productFeaturesMap.getOrElse(item, Array.empty)}
          //根据用户评分记录vector，计算记录中各movie的相似movie
          val similars: Array[(String, Double)] = getHistorySimilar(queryFeatures, model.productFeaturesMap).map(k => (itemIntStringMap(k._1), k._2)).filter(k => !history.contains(k._1))
          val similarTopN: Array[(String, Double)] = getTopN(similars, 3 * query.num)(ord).toArray
          val temp = (als ++ similarTopN).groupBy(_._1).map(k => (k._1, k._2.map(s => s._2).sum)).toArray.sortWith(_._2>_._2)
          if(temp.length < query.num){
            val re = (temp ++ model.movieSortByRate.take(3 * (query.num - temp.length))).groupBy(_._1).map(k => (k._1, k._2.map(s => s._2).sum)).toArray.sortWith(_._2>_._2)
            PredictedResult(re.map(k => ItemScore(k._1, k._2)))
          }
          else{
            PredictedResult(temp.take(query.num).map(k => ItemScore(k._1, k._2)))
          }
        }
        else{
          //按评分高低取
          PredictedResult(model.movieSortByRate.take(query.num).map(k => ItemScore(k._1, k._2)))
        }
      }
      else {
        PredictedResult(als.map(k => ItemScore(k._1, k._2)))
      }
    }
    else {
      //查询用户评分过movie
      val ord = Ordering.by[(String, Double), Double](_._2).reverse
      val queryFeatures: Vector[Array[Double]] = Array[Array[Double]](model.productFeaturesMap.getOrElse(model.itemStringIntMap.getOrElse(query.item.getOrElse("none"), -10000),Array.empty)).toVector
      val itemIntStringMap: BiMap[Int, String] = model.itemStringIntMap.inverse
      val similars = getHistorySimilar(queryFeatures, model.productFeaturesMap).map(k => (itemIntStringMap(k._1), k._2)).filter(k => !k._1.equals(query.item.getOrElse("none")))
      val similarTopN = getTopN(similars, query.num)(ord).toArray.map(k => ItemScore(k._1, k._2))
      PredictedResult(similarTopN)
    }
  }

}
