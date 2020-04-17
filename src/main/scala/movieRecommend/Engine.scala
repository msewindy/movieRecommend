package movieRecommend

import org.apache.predictionio.controller.{Engine, EngineFactory}

case class Query(
  user: Option[String],
  num: Int,
  item: Option[String]
)

case class PredictedResult(
  itemScores: Array[ItemScore]
)

case class ItemScore(
  item: String,
  score: Double
)

object RecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("als" -> classOf[ALSAlgorithm]),
      classOf[Serving])
  }
}
