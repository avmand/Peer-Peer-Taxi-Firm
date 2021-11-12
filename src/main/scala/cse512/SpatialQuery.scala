package cse512
import scala.math.{min, pow, sqrt,max}
import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Within(pointString1:String, pointString2:String,distance:Double): Boolean ={
    if(pointString1.isEmpty() || pointString2.isEmpty() || pointString1 == null || pointString2 == null || distance<=0.00)
      return false

    val pointString1SPlit = pointString1.split(",")
    val x1 = pointString1SPlit(0).toDouble
    val y1 = pointString1SPlit(1).toDouble

    val pointString2SPlit = pointString2.split(",")
    val x2 = pointString2SPlit(0).toDouble
    val y2 = pointString2SPlit(1).toDouble

    // Euclidean distance between 2 points
    val distanceBwPoints = sqrt(pow((x1-x2),2) + pow((y1-y2),2))

    if(distanceBwPoints <= distance) return true
    else return false
  }

  def ST_Contains(queryRectangle: String,pointString: String): Boolean = {
    if (pointString.isEmpty()) {
      return false
    }
    else if (pointString == null) {
      return false
    }
    else if (queryRectangle.isEmpty()) {
      return false
    }
    else if (queryRectangle == null) {
      return false
    }
    val ptStrSplit = pointString.split(",")
    val ptX = ptStrSplit(0).toDouble
    val ptY = ptStrSplit(1).toDouble
    val qRectSplit = queryRectangle.split(",")
    val upLX = qRectSplit(0).toDouble
    val upLY = qRectSplit(1).toDouble
    val btmRX = qRectSplit(2).toDouble
    val btmRY = qRectSplit(3).toDouble

    val minX = min(upLX,btmRX)
    val maxX = max(upLX,btmRX)
    val minY = min(upLY,btmRY)
    val maxY = max(upLY,btmRY)

    // Checking if point lies within rectangle
    if(ptX>=minX && ptX<=maxX && ptY >= minY && ptY <= maxY)  return true
    return false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle,pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle,pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}