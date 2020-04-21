import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.{File, PrintWriter}

import org.locationtech.geomesa.spark.jts._

import scala.math._

object TestGeomesa {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
                  .appName("geomesa-spark profile")
                  .getOrCreate()
                  .withJTS

    import spark.implicits._
    var dataPath = "/home/zc/tests/data/10_3/"
    var outputPath = "/home/zc/tests/data/output/"
    var hdfsPath = "hdfs://spark1:9000"
    var isHDFS = false
    var func = ""
    var dataNums = 100
    var runTime = 6
    var isShow = false
    val argsList = args.toList
    val argsMap = Parse.nextOption(Map(), argsList)
    if(argsMap.contains('inputCsvPath)) {
      dataPath = argsMap('inputCsvPath)
    }
    if(argsMap.contains('outputLogPath)){
      outputPath = argsMap('outputLogPath)
    }
    if(argsMap.contains('functionName)){
      func = argsMap('functionName)
    }
    if(argsMap.contains('dataNums)){
      dataNums = argsMap('dataNums).toInt
    }
    if(argsMap.contains('runTime)){
      runTime = argsMap('runTime).toInt
    }
    if(argsMap.contains('isShowDF)){
      isShow = argsMap('isShowDF).toBoolean
    }

    val dataPower = Math.log10(dataNums).toInt
    dataPath = dataPath + "10_" + dataPower.toString + "/"
    outputPath = outputPath + "10_" + dataPower.toString + "/"

    if (dataPath.startsWith("hdfs")){
      isHDFS = true
    }
    if (isHDFS){
      val strList = dataPath.split("/", 0)
      hdfsPath = strList(0) + "//" + strList(2)
      val realPath = hdfsPath + outputPath
      val conf = new Configuration()
      conf.set("fs.defaultFS", hdfsPath)
      val fs = FileSystem.get(conf)
      if (!fs.exists(new Path(realPath))){
        fs.mkdirs(new Path(realPath))
      }
    } else {
      val fp = new File(outputPath)
      if (!fp.exists()){
        fp.mkdirs()
      }
    }

    var begin = System.nanoTime
    var end = System.nanoTime

    def runSql(sql : String): Unit={
      val df = spark.sql(sql)
      if(isShow){
        df.show(20, 0)
      }
      df.createOrReplaceTempView("result")
      spark.sql("CACHE TABLE result")
      spark.sql("UNCACHE TABLE result")
    }

    def writeTime(funcName : String, durTimeArray : Array[Double]): Unit ={
      if (isHDFS){
        val conf = new Configuration()
        conf.set("fs.defaultFS", hdfsPath)
        val fs = FileSystem.get(conf)
        val output = fs.create(new Path(outputPath + funcName + ".txt"))
        val writer = new PrintWriter(output)
        try {
          val i = 0
          for (i <- 0 to runTime - 1) {
            writer.println("geomesa_" + funcName + "_time:" + durTimeArray(i))
          }
        }
        finally {
          writer.close()
        }
      } else {
        val writer = new PrintWriter(outputPath + funcName + ".txt")
        val i = 0
        for (i <- 0 to runTime - 1) {
          writer.println("geomesa_" + funcName + "_time:" + durTimeArray(i))
        }
        writer.close()
      }
    }
    
    def calculateTime(sql : String, funcName : String): Unit ={
      var i = 0
      val durTimeArray = new Array[Double](runTime)
      for(i <- 0 to runTime - 1){
        begin = System.nanoTime()
        runSql(sql)
        end = System.nanoTime()
        val durTime = (end - begin) / 1e9d
        durTimeArray(i) = durTime
      }
      writeTime(funcName, durTimeArray)
    }

    def filePathMap(funcName : String):String = funcName match {
        case "st_area" | "st_astext" | "st_envelope" | "st_geomfromwkt" | "st_issimple" | "st_polygonfromtext" => "single_polygon"
        case "st_pointfromtext" | "st_buffer" => "single_point"
        case "st_length" | "st_linestringfromtext" => "single_linestring"
        case "st_isvalid" | "st_npoints" | "st_centroid" | "st_geometry_type" => "single_col"
        case "st_intersects" | "st_overlaps" | "st_contains" | "st_equals" | "st_crosses" | "st_touches" => "double_col"
        case "st_distance" => "st_distance"
        case "st_within" => "st_within"
        case "st_point" => "st_point"
      }
    
    def TestSTPoint(): Unit ={
      val funcName = "st_point"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("x double, y double").load(filePath).cache()
      dataDF.createOrReplaceTempView("points")
//       dataDF.show(20,0)
      val sql = "select ST_Point(x, y) from points"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    Geomesa doesn't have this function
//    def test_st_geomfromgeojson()

    def TestSTPointFromText(): Unit ={
      val funcName = "st_pointfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
//       dataDF.show(20,0)
      val sql = "select ST_PointFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTPolygonFromText(): Unit ={
      val funcName = "st_polygonfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
//       dataDF.show(20,0)
      val sql = "select ST_PolygonFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTAsText(): Unit ={
      val funcName = "st_astext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
//       dataDF.show(20,0)
      val sql = "select ST_AsText(ST_GeomFromText(data)) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTPrecisionReduce   Geomesa has no this

    def TestSTLineStringFromText(): Unit ={
      val funcName = "st_linestringfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
//       dataDF.show(20,0)
      val sql = "select ST_LineFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeomFromWKT(): Unit ={
      val funcName = "st_geomfromwkt"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
//       dataDF.show(20,0)
      val sql = "select ST_GeomFromWKT(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeomFromText(): Unit ={
      val funcName = "st_geomfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
//       dataDF.show(20,0)
      val sql = "select ST_GeomFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTIntersection  Geomesa has no this func

    def TestSTIsValid(): Unit ={
      val funcName = "st_isvalid"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("valid")
//       dataDF.show(20,0)
      val sql = "select ST_IsValid(ST_GeomFromText(geos)) from valid"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTEquals(): Unit ={
      val funcName = "st_equals"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("equals")
//       dataDF.show(20,0)
      val sql = "select ST_Equals(ST_GeomFromText(left), ST_GeomFromText(right)) from equals"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTTouches(): Unit ={
      val funcName = "st_touches"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("touches")
//       dataDF.show(20,0)
      val sql = "select ST_Touches(ST_GeomFromText(left), ST_GeomFromText(right)) from touches"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTOverlaps(): Unit ={
      val funcName = "st_overlaps"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("overlaps")
//       dataDF.show(20,0)
      val sql = "select ST_Overlaps(ST_GeomFromText(left), ST_GeomFromText(right)) from overlaps"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTCrosses(): Unit ={
      val funcName = "st_crosses"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("crosses")
//       dataDF.show(20,0)
      val sql = "select ST_Crosses(ST_GeomFromText(left), ST_GeomFromText(right)) from crosses"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTIsSimple(): Unit ={
      val funcName = "st_issimple"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("simple")
//       dataDF.show(20,0)
      val sql = "select ST_IsSimple(ST_GeomFromText(geos)) from simple"
     calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeometryType(): Unit ={
      val funcName = "st_geometry_type"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("geometry_type")
//       dataDF.show(20,0)
      val sql = "select ST_GeometryType(ST_GeomFromText(geos)) from geometry_type"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTMakeValid()  Geomesa has no this func
//    def TestSTMakeValid(): Unit ={
//      var filePath = dataPath.concat("st_make_valid.csv")
//      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
//      dataDF.createOrReplaceTempView("make_valid")
////       dataDF.show(20,0)
//      val sql = "select ST_MakeValid(ST_GeomFromText(geos)) from make_valid"
//      begin = System.nanoTime
//      runSql(sql)
//      end = System.nanoTime
//      println("geomesa_st_make_valid_time: " + (end - begin) / 1e9d)
//      dataDF.unpersist()
//    }

//    TestSTSimplifyPreserveTopology    Geomesa has no this func
//    TestSTPolygonFromEnvelope Geomesa has no this func

    def TestSTContains(): Unit ={
      val funcName = "st_contains"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("contains")
//       dataDF.show(20,0)
      val sql = "select ST_Contains(ST_GeomFromText(left), ST_GeomFromText(right)) from contains"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTIntersects(): Unit ={
      val funcName = "st_intersects"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("intersects")
//       dataDF.show(20,0)
      val sql = "select ST_Intersects(ST_GeomFromText(left), ST_GeomFromText(right)) from intersects"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTWithIn(): Unit ={
      val funcName = "st_within"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("within")
//       dataDF.show(20,0)
      val sql = "select ST_Within(ST_GeomFromText(left), ST_GeomFromText(right)) from within"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTDistance(): Unit ={
      val funcName = "st_distance"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("distance")
//       dataDF.show(20,0)
      val sql = "select ST_Distance(ST_GeomFromText(left), ST_GeomFromText(right)) from distance"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTArea(): Unit ={
      val funcName = "st_area"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("area")
//       dataDF.show(20,0)
      val sql = "select ST_Area(ST_GeomFromText(geos)) from area"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTCentroid(): Unit ={
      val funcName = "st_centroid"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("centroid")
//       dataDF.show(20,0)
      val sql = "select ST_Centroid(ST_GeomFromText(geos)) from centroid"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTLength(): Unit ={
      val funcName = "st_length"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("length")
//       dataDF.show(20,0)
      val sql = "select ST_Length(ST_GeomFromText(geos)) from length"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTHausdorffDistance  Geometry has no this func

    def TestSTConvexHull(): Unit ={
      val funcName = "st_test"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("convexhull")
//       dataDF.show(20,0)
      val sql = "select ST_ConvexHull(ST_GeomFromText(geos)) from convexhull"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTNPoints(): Unit ={
      val funcName = "st_npoints"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("npoints")
//       dataDF.show(20,0)
      val sql = "select ST_NumPoints(ST_GeomFromText(geos)) from npoints"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTEnvelope(): Unit ={
      val funcName = "st_envelope"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("envelope")
//       dataDF.show(20,0)
      val sql = "select ST_Envelope(ST_GeomFromText(geos)) from envelope"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTBuffer(): Unit ={
      val funcName = "st_buffer"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("buffer")
//       dataDF.show(20,0)
      val sql = "select ST_BufferPoint(ST_GeomFromText(geos), 1.2) from buffer"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTUnionAggr()
//    TestSTEnvelopeAggr()

//    def TestSTTranslate(): Unit ={
//      val funcName = "st_transform"
//      println(funcName)
//      val filePath = dataPath + fileName
//      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
//      dataDF.createOrReplaceTempView("translate")
////       dataDF.show(20,0)
//      val sql = "select ST_Translate(ST_GeomFromText(geos), 'epsg:4326', 'epsg:3857') from translate"
//      calculateTime(sql, funcName)
//      dataDF.unpersist()
//    }

    val testcases = Map(
      "Point"->(() =>             TestSTPoint()),
      "PointFromText"->(() =>     TestSTPointFromText()),
      "Area"->(() =>              TestSTArea()),
      "AsText"->(() =>            TestSTAsText()),
      "Centroid"->(() =>          TestSTCentroid()),
      "Contains"->(() =>          TestSTContains()),
//      "ConvexHull"->(() =>         TestSTConvexHull()),
      "Crosses"->(() =>           TestSTCrosses()),
      "Distance"->(() =>          TestSTDistance()),
      "Equals"->(() =>            TestSTEquals()),
      "GeometryType"->(() =>      TestSTGeometryType()),
      "GeomFromText"->(() =>      TestSTGeomFromText()),
      "GeomFromWKT"->(() =>       TestSTGeomFromWKT()),
      "Intersects"->(() =>        TestSTIntersects()),
      "IsSimple"->(() =>          TestSTIsSimple()),
      "IsValid"->(() =>           TestSTIsValid()),
      "Length"->(() =>            TestSTLength()),
      "LineStringFromText"->(() =>TestSTLineStringFromText()),
//      "MakeValid"->(() =>         TestSTMakeValid()),
      "NPoints"->(() =>           TestSTNPoints()),
      "Touches"->(() =>           TestSTTouches()),
      "Overlaps"->(() =>          TestSTOverlaps()),
      "WithIn"->(() =>            TestSTWithIn()),
      "PolygonFromText"->(() =>   TestSTPolygonFromText()),
      "Envelope"->(() =>          TestSTEnvelope()),
      "Buffer"->(() =>            TestSTBuffer()),
//      "Translate"->(() =>         TestSTTranslate()),
    )

    testcases(func).apply

    // TODO: TestSTMakeValid, TestSTConvehull's sql
  }
}

object Parse {
  val usage =
    """
    Usage: spark-submit --master yarn --deploy-mode client --jars ./dependencies/jars  --class TestGeomesa ./target/xx.jar [-h] [-p] <inputCsvPath> [-o] <outputPath> [-f] functionName [-n] dataNums [-t] runTime [-s] isShowDF
    """
  type OptionMap = Map[Symbol, String]

  def nextOption(map: OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "-h" :: other =>
        nextOption(map ++ Map('help -> usage), other)
      case "-p" :: value :: tail =>
        nextOption(map ++ Map('inputCsvPath -> value.toString), tail)
      case "-o" :: value :: tail =>
        nextOption(map ++ Map('outputPath -> value.toString), tail)
      case "-f" :: value :: tail =>
        nextOption(map ++ Map('functionName -> value.toString), tail)
      case "-n" :: value :: tail =>
        nextOption(map ++ Map('dataNums -> value.toString), tail)
      case "-t" :: value :: tail =>
        nextOption(map ++ Map('runTime -> value.toString), tail)
      case "-s" :: value :: tail =>
        nextOption(map ++ Map('isShowDF -> value.toString), tail)
      case option :: tail =>
        nextOption(map, tail)
    }
  }
}

