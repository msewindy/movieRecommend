package movieRecommend

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.{EmptyActualResult, EmptyEvaluationInfo, PDataSource, Params}
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(
  appName: String,
  evalParams: Option[DataSourceEvalParams]) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  def getRatings(sc: SparkContext): RDD[Rating] = {
    //读取mysql数据库中用户对视频的评分
    val sqlContext: DataFrame = new SQLContext(sc).read
      .format("jdbc")
      .option("url", "jdbc:mysql://47.97.59.61:3306/video_resource_playback_site_sp_ly")
      .option("dbtable", "video_resource_playback_site_sp_ly.t_bus_raty")
      .option("user", "root")
      .option("password", "YS.123456")
      .load()
    sqlContext.rdd.map(k => new Rating(k.get(0).toString, k.get(2).toString, k.get(3).toString.toDouble))
  }

  override
  def readTraining(sc: SparkContext): TrainingData = {
    new TrainingData(getRatings(sc))
  }

}

case class Rating(
  user: String,
  item: String,
  rating: Double
)

class TrainingData(
  val ratings: RDD[Rating]
) extends Serializable {
  override def toString = {
    s"ratings: [${ratings.count()}] (${ratings.take(2).toList}...)"
  }
}
