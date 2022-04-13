package com.ouning.streaming

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @Description TODO
 * @author on
 * @since 2022年01月26日 21:20:30
 */
// 连接助手对象
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("ERS")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://ERS:27017/recommender"))
}

case class MongConfig(uri: String, db: String)

// 标准推荐
case class Recommendation(productId: Int, score: Double)

// 用户的推荐
case class UserRecs(userId: Int, recs: Seq[Recommendation])

//商品的相似度
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  //入口方法  实时推荐主体代码
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://ERS:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    implicit val mongConfig = MongConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._

    // 广播商品相似度矩阵
    //装换成为 Map[Int, Map[Int,Double]]
    val simProductsMatrix = spark
      .read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map { recs =>
        (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)
      }.collectAsMap()

    val simProductsMatrixBroadCast = sc.broadcast(simProductsMatrix)

    //创建到Kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    // UID|MID|SCORE|TIMESTAMP
    // 产生评分流
    val ratingStream = kafkaStream.map { case msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
    }

    // 核心实时推荐算法(定义评分流的处理流程)
    ratingStream.foreachRDD { rdd =>
      rdd.map { case (userId, productId, score, timestamp) =>
        println(">>>>>>>>>>>>>>>>")
        //1.取出redis当前用户的最近评分，
        // 保存成一个数组Array[(productId, score)]（获取当前最近的M次商品评分）
        val userRecentlyRatings = getUserRecentlyRating(
          MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)
        //2.从相似度矩阵中获取当前商品最相似的商品列表，作为备选列表，
        // 保存成一个数组Array[(productId, score)](获取商品P最相似的K个商品)
        val simProducts = getTopSimProducts(
          MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBroadCast.value)
        //3.计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表，
        // （保存成一个数组Array[(productId, score)]计算待选商品的推荐优先级）
        val streamRecs = computeProductScores(
          simProductsMatrixBroadCast.value, userRecentlyRatings, simProducts)
        //4.把推荐列表保存到MongoDB
        saveRecsToMongoDB(userId, streamRecs)
      }.count()
    }

    //启动Streaming程序
    ssc.start()
    ssc.awaitTermination()
  }

  import scala.collection.JavaConversions._
  /**
   * 获取当前最近的M次商品评分
   * 从用户的队列中取出num个评分
   * @param num    评分的个数
   * @param userId 谁的评分
   * @return
   */
  def getUserRecentlyRating(num: Int, userId: Int, jedis: Jedis):
  Array[(Int, Double)] = {
    //从redis中用户的评分队列里获取评分数据，list键名为uid:USERID,
    //值格式是 PRODUCTID:SCORE
    jedis.lrange("userId:" + userId.toString, 0, num)
      .map { item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }


  /**
   * 获取当前商品K个相似的商品
   *
   * @param num         相似商品的数量
   * @param productId   当前商品的ID
   * @param userId      当前的评分用户
   * @param simProducts 商品相似度矩阵的广播变量值
   * @param mongConfig  MongoDB的配置
   * @return
   */
  //获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
  def getTopSimProducts(num: Int, productId: Int, userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongConfig: MongConfig): Array[Int] = {
    //从广播变量的商品相似度矩阵中获取当前商品所有的相似商品
    val allSimProducts = simProducts.get(productId).get.toArray
    //获得用户已经评分过的商品，过滤掉，排序输出
    val ratingcollection = ConnHelper
      .mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION)
    val ratingExist = ratingcollection.find(MongoDBObject("userId" -> userId))
      .toArray // 只需要productId
      .map { item => item.get("productId").toString.toInt}
    //过滤掉已经评分过得商品，并排序输出
    allSimProducts.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2).take(num).map(x => x._1)
  }

  /**
   * 计算待选商品的推荐分数
   *
   * @param simProducts         商品相似度矩阵
   * @param userRecentlyRatings 用户最近的k次评分
   * @param topSimProducts      当前商品最相似的K个商品
   * @return
   */
  //计算待选商品的推荐分数
  def computeProductScores(simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
                           userRecentlyRatings: Array[(Int, Double)], topSimProducts: Array[Int]):
  Array[(Int, Double)] = {
    //定义一个长度可变数组ArrayBuffer，
    // 用于保存每一个待选商品和最近评分的每一个商品的权重(基础)得分，(productId, score)
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义两个map, 用于保存每个商品的高分和低分的计数器，productId -> count
    //用于保存每一个商品的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    //用于保存每一个商品的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (topSimProduct <- topSimProducts; userRecentlyRating <- userRecentlyRatings) {
      // 从相似度矩阵中获取当前备选商品和已评分商品间的相似度
      val simScore = getProductsSimScore(simProducts, userRecentlyRating._1, topSimProduct)
      if (simScore > 0.6) {  // 按照公式进行加权计算， 得到基础评分
        score += ((topSimProduct, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct, 0) + 1
        } else {
          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct, 0) + 1
        }
      }
    }
    // 根据公式计算所有的推荐优先级，首先以productId做groupby
    val tuples = score.groupBy(_._1).map { case (productId, sims) =>
      (productId, sims.map(_._2).sum / sims.length
        + log(increMap.getOrDefault(productId, 1))
        - log(decreMap.getOrDefault(productId, 1)))
    }.toArray.sortWith(_._2 > _._2)
    // 展示推荐列表
    tuples.foreach(println)
    println("===================分割线======================")
    // 返回推荐列表
    tuples

  }
  // 其中，getProductSimScore是取候选商品和已评分商品的相似度，代码如下：

  /**
   * 获取当个商品之间的相似度
   *
   * @param simProducts       商品相似度矩阵
   * @param userRatingProduct 用户已经评分的商品
   * @param topSimProduct     候选商品
   * @return
   */
  def getProductsSimScore(
         simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]],
         userRatingProduct: Int,
         topSimProduct: Int): Double = {
    simProducts.get(topSimProduct) match {
      case Some(sim) => sim.get(userRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 而log是对数运算，这里实现为取10的对数（常用对数）：
  //取10的对数
  // 自定义log函数，以N为底
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N)
  }

  /**
   * 将数据保存到MongoDB    userId -> 1,  recs -> 22:4.5|45:3.8
   * @param streamRecs  流式的推荐结果
   * @param mongConfig  MongoDB的配置
   */
  def saveRecsToMongoDB(userId:Int,streamRecs:Array[(Int,Double)])(implicit mongConfig: MongConfig): Unit ={
    //到StreamRecs的连接
    val streaRecsCollection = ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streaRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" ->
      streamRecs.map( x => MongoDBObject("productId"->x._1,"score"->x._2)) ))
  }

}

