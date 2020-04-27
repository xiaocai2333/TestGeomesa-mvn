import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io._ 

import java.text.SimpleDateFormat
import java.util.Calendar

import javax.rmi.CORBA.Util
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
    var func = ""
    var dataNums = 100
    var runTime = 6
    var isShow = false
    val argsList = args.toList
    val argsMap = Parse.nextOption(Map(), argsList)
    if (argsMap.contains('inputCsvPath)) {
      dataPath = argsMap('inputCsvPath)
    }
    if (argsMap.contains('outputPath)) {
      outputPath = argsMap('outputPath)
    }
    if (argsMap.contains('functionName)) {
      func = argsMap('functionName)
    }
    if (argsMap.contains('dataNums)) {
      dataNums = argsMap('dataNums).toInt
    }
    if (argsMap.contains('runTime)) {
      runTime = argsMap('runTime).toInt
    }
    if (argsMap.contains('isShowDF)) {
      isShow = argsMap('isShowDF).toBoolean
    }

    val dataPower = Math.log10(dataNums).toInt
    dataPath = dataPath + "10_" + dataPower.toString + "/"
    outputPath = outputPath + "10_" + dataPower.toString + "/"
    var isHDFS = dataPath.startsWith("hdfs")
    var myDF : MyFS = if (isHDFS) new MyHDFS(outputPath) else new MyLocalFS(outputPath)

    var begin = System.nanoTime
    var end = System.nanoTime

    def runSql(sql: String): Unit = {
      val df = spark.sql(sql)
      if (isShow) {
        df.show(20, 0)
      }
      df.createOrReplaceTempView("result")
      spark.sql("CACHE TABLE result")
      spark.sql("UNCACHE TABLE result")
    }

    def getCurrentTime():String = {
         val formatDate = new SimpleDateFormat("yyyy-MM-dd:hhmmss")
         val now  = Calendar.getInstance().getTime
         formatDate.format(now)
    }

    def calculateTime(sql: String, funcName: String): Unit = {
      var i = 0
      val durTimeArray = new Array[Double](runTime)
      for (i <- 0 to runTime - 1) {
        begin = System.nanoTime()
        runSql(sql)
        end = System.nanoTime()
        val durTime = (end - begin) / 1e9d
        durTimeArray(i) = durTime
      }
      myDF.myWrite(funcName, durTimeArray)
    }

    def filePathMap(funcName: String): String = funcName match {
      case "st_area" | "st_astext" | "st_envelope" | "st_geomfromwkt" | "st_issimple" | "st_polygonfromtext" => "single_polygon"
      case "st_pointfromtext" | "st_buffer" => "single_point"
      case "st_length" | "st_linestringfromtext" => "single_linestring"
      case "st_isvalid" | "st_npoints" | "st_centroid" | "st_geometry_type" => "single_col"
      case "st_intersects" | "st_overlaps" | "st_contains" | "st_equals" | "st_crosses" | "st_touches" => "double_col"
      case "st_distance" => "st_distance"
      case "st_within" => "st_within"
      case "st_point" => "st_point"
    }

    def TestSTPoint(): Unit = {
      val funcName = "st_point"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("x double, y double").load(filePath).cache()
      dataDF.createOrReplaceTempView("points")
      val sql = "select ST_Point(x, y) from points"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    //    Geomesa doesn't have this function
    //    def test_st_geomfromgeojson()

    def TestSTPointFromText(): Unit = {
      val funcName = "st_pointfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql = "select ST_PointFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTPolygonFromText(): Unit = {
      val funcName = "st_polygonfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql = "select ST_PolygonFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTAsText(): Unit = {
      val funcName = "st_astext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("asText")
      spark.sql("CACHE TABLE asText")
      val sql = "select ST_AsText(geos) from asText"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    //    TestSTPrecisionReduce   Geomesa has no this

    def TestSTLineStringFromText(): Unit = {
      val funcName = "st_linestringfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql = "select ST_LineFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeomFromWKT(): Unit = {
      val funcName = "st_geomfromwkt"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql = "select ST_GeomFromWKT(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeomFromText(): Unit = {
      val funcName = "st_geomfromtext"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql = "select ST_GeomFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    //    TestSTIntersection  Geomesa has no this func

    def TestSTIsValid(): Unit = {
      val funcName = "st_isvalid"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("isValid")
      spark.sql("CACHE TABLE isValid")
      val sql = "select ST_IsValid(geos) from isValid"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTEquals(): Unit = {
      val funcName = "st_equals"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("equals")
      spark.sql("CACHE TABLE equals")
      val sql = "select ST_Equals(left_geos, right_geos) from equals"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTTouches(): Unit = {
      val funcName = "st_touches"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("touches")
      spark.sql("CACHE TABLE touches")
      val sql = "select ST_Touches(left_geos, right_geos) from touches"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTOverlaps(): Unit = {
      val funcName = "st_overlaps"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("overlaps")
      spark.sql("CACHE TABLE overlaps")
      val sql = "select ST_Overlaps(left_geos, right_geos) from overlaps"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTCrosses(): Unit = {
      val funcName = "st_crosses"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("crosses")
      spark.sql("CACHE TABLE crosses")
      val sql = "select ST_Crosses(left_geos, right_geos) from crosses"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def writeFile(filename: String, s: String): Unit = {
      val file = new File(filename)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(s)
      bw.close()
    }

    def TestSTIsSimple(): Unit = {
      val funcName = "st_issimple"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("simple")
      spark.sql("CACHE TABLE simple")
      val sql = "select ST_IsSimple(geos) from simple"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
      val line = getCurrentTime()
      writeFile("/home/zilliz/mesa_issimple", line)
    }

    def TestSTGeometryType(): Unit = {
      val funcName = "st_geometry_type"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("geometryType")
      spark.sql("CACHE TABLE geometryType")
      val sql = "select ST_GeometryType(geos) from geometryType"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTContains(): Unit = {
      val funcName = "st_contains"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("contains")
      spark.sql("CACHE TABLE contains")
      val sql = "select ST_Contains(left_geos, right_geos) from contains"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTIntersects(): Unit = {
      val funcName = "st_intersects"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("intersects")
      spark.sql("CACHE TABLE intersects")
      val sql = "select ST_Intersects(left_geos, right_geos) from intersects"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTWithIn(): Unit = {
      val funcName = "st_within"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("withIn")
      spark.sql("CACHE TABLE withIn")
      val sql = "select ST_Within(left_geos, right_geos) from withIn"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTDistance(): Unit = {
      val funcName = "st_distance"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(left) as left_geos, ST_GeomFromText(right) as right_geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("distance")
      spark.sql("CACHE TABLE distance")
      val sql = "select ST_Distance(left_geos, right_geos) from distance"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTArea(): Unit = {
      val funcName = "st_area"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1 = "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("area")
      spark.sql("CACHE TABLE area")
      val sql = "select ST_Area(geos) from area"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTCentroid(): Unit = {
      val funcName = "st_centroid"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("centroid")
      spark.sql("CACHE TABLE centroid")

      val sql = "select ST_Centroid(geos) from centroid"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTLength(): Unit = {
      val funcName = "st_length"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("length")
      spark.sql("CACHE TABLE length")
      val sql = "select ST_Length(geos) from length"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTConvexHull(): Unit = {
      val funcName = "st_convexhull"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("convexhull")
      spark.sql("CACHE TABLE convexhull")
      val sql = "select ST_ConvexHull(ST_GeomFromText(geos)) from convexhull"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTNPoints(): Unit = {
      val funcName = "st_npoints"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("npoints")
      spark.sql("CACHE TABLE npoints")
      val sql = "select ST_NumPoints(geos) from npoints"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTEnvelope(): Unit = {
      val funcName = "st_envelope"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("envelope")
      spark.sql("CACHE TABLE envelope")
      val sql = "select ST_Envelope(geos) from envelope"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    def TestSTBuffer(): Unit = {
      val funcName = "st_buffer"
      println(funcName)
      val fileName = filePathMap(funcName) + ".csv"
      val filePath = dataPath + fileName
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      val sql1= "select ST_GeomFromText(data) as geos from data"
      val tmpDF = spark.sql(sql1)
      tmpDF.createOrReplaceTempView("buffer")
      spark.sql("CACHE TABLE buffer")
      val sql = "select ST_BufferPoint(geos, 1.2) from buffer"
      calculateTime(sql, funcName)
      tmpDF.unpersist()
      dataDF.unpersist()
    }

    val testcases = Map(
      "Point" -> (() => TestSTPoint()),
      "PointFromText" -> (() => TestSTPointFromText()),
      "Area" -> (() => TestSTArea()),
      "AsText" -> (() => TestSTAsText()),
      "Centroid" -> (() => TestSTCentroid()),
      "Contains" -> (() => TestSTContains()),
//      "ConvexHull"->(() =>         TestSTConvexHull()),
      "Crosses" -> (() => TestSTCrosses()),
      "Distance" -> (() => TestSTDistance()),
      "Equals" -> (() => TestSTEquals()),
      "GeometryType" -> (() => TestSTGeometryType()),
//      "GeomFromText" -> (() => TestSTGeomFromText()),
      "GeomFromWKT" -> (() => TestSTGeomFromWKT()),
      "Intersects" -> (() => TestSTIntersects()),
      "IsSimple" -> (() => TestSTIsSimple()),
      "IsValid" -> (() => TestSTIsValid()),
      "Length" -> (() => TestSTLength()),
      "LineStringFromText" -> (() => TestSTLineStringFromText()),
//      "MakeValid"->(() =>         TestSTMakeValid()),
      "NPoints" -> (() => TestSTNPoints()),
      "Touches" -> (() => TestSTTouches()),
      "Overlaps" -> (() => TestSTOverlaps()),
      "WithIn" -> (() => TestSTWithIn()),
      "PolygonFromText" -> (() => TestSTPolygonFromText()),
      "Envelope" -> (() => TestSTEnvelope()),
      "Buffer" -> (() => TestSTBuffer()),
//      "Translate"->(() =>         TestSTTranslate()),
    )

    testcases(func).apply

  }
}

class MyFS() {
  def myWrite(funcName: String, durTimeArray: Array[Double]): Unit = {
  }
}

class MyLocalFS(outputPath: String) extends MyFS {
  val realPath = outputPath

  def mkdirDirectory(): Unit = {
    val fp = new File(realPath)
    if (!fp.exists()) {
      fp.mkdirs()
    }
  }

  mkdirDirectory()

  override def myWrite(funcName: String, durTimeArray: Array[Double]): Unit = {
    val writer = new PrintWriter(realPath + funcName + ".txt")
    val i = 0
    for (i <- 0 to durTimeArray.length - 1) {
      writer.println("geomesa_" + funcName + "_time:" + durTimeArray(i))
    }
    writer.close()
  }
}

class MyHDFS(outputPath: String) extends MyFS {
  var hdfsPath = "hdfs://spark1:9000"
  var realPath: String = "/test_data/"

  def parseHDFS(): Unit = {
    var strList = outputPath.split("/", -1)
    hdfsPath = strList(0) + "//" + strList(2)
    realPath = "/" + strList.takeRight(strList.length - 3).mkString("/")
  }

  parseHDFS()

  val conf = new Configuration()
  conf.set("fs.defaultFS", hdfsPath)
  val fs = FileSystem.get(conf)

  def mkdirDirectory(): Unit = {
    if (!fs.exists(new Path(realPath))) {
      fs.mkdirs(new Path(realPath))
    }
  }

  mkdirDirectory()

  override def myWrite(funcName: String, durTimeArray: Array[Double]): Unit = {
    val output = fs.create(new Path(realPath + funcName + ".txt"))
    val writer = new PrintWriter(output)
    try {
      val i = 0
      for (i <- 0 to durTimeArray.length - 1) {
        writer.println("geomesa_" + funcName + "_time:" + durTimeArray(i))
      }
    }
    finally {
      writer.close()
    }
  }
}

object Parse {
  val usage =
    """
    Usage: spark-submit --master yarn --deploy-mode client --jars ./dependencies/jars  --class TestGeomesa ./target/xx.jar [-h] [-p] <inputCsvPath> [-o] <outputPath> [-f] functionName [-n] dataNums [-t] runTime [-s] isShowDF
    """
  type OptionMap = Map[Symbol, String]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
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

