package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(queryRectangle: String,pointString: String): Boolean = {
    if (pointString == null) {
      return false
    }

    else if (queryRectangle == null) {
      return false
    }
    val ptStrSplit = pointString.split(",")
    val ptX = ptStrSplit(0).toDouble
    val ptY = ptStrSplit(1).toDouble
    val qRectSplit = queryRectangle.split(",")
    print(ptStrSplit(0).toDouble)
    print("\n")
    print(qRectSplit(0).toDouble)
    print("\n")
    return false
    //val upLX = qRectSplit(0)
    //val upLY = qRectSplit(1)
    //val btmRX = qRectSplit(2)
    //val btmRY = qRectSplit(3)

    /*if (((upLX <= ptX) && (ptX <= btmRX) && (upLY >= ptY) && (ptY >= btmRY))||((btmRX >= ptX) && (ptX <= upLX) && (btmRY <= ptY) && (ptY <= upLY))) {
      return true
    }*/
    val upX = 0.0
    val lowX =0.0
    val upY = 0.0
    val lowY = 0.0
    if ( qRectSplit(0) > qRectSplit(2)) {
      val upX = qRectSplit(0).toDouble
      val lowX = qRectSplit(2).toDouble
    }
    else{
      val upX = qRectSplit(2).toDouble
      val lowX = qRectSplit(0).toDouble
    }

    if ( qRectSplit(1) > qRectSplit(3)){
      val upY = qRectSplit(1).toDouble
      val lowY = qRectSplit(3).toDouble
    }else {
      val upY = qRectSplit(3).toDouble
      val lowY = qRectSplit(1).toDouble
    }
    if ((upX >= ptX) && (ptX >= lowX) && (upY >= ptY) && (ptY >= lowY)) {
      return true
    }
    else
      return false
  }



  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle,pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()
    print(resultDf.count())
    print("\n")
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
    print(resultDf.count())
    print("\n")

    return resultDf.count()
  }

  /*
  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((true)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }*/
}
