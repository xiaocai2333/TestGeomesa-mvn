import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.io.PrintWriter

import org.locationtech.geomesa.spark.jts._

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
    if (args.length > 0){
      dataPath = args(0)
      outputPath = args(1)
      func = args(2)
    }
    
    var begin = System.nanoTime
    var end = System.nanoTime

    def runSql(sql : String): Unit={
      val df = spark.sql(sql)
//      df.show(20, 0)
      df.createOrReplaceTempView("result")
      spark.sql("CACHE TABLE result")
      spark.sql("UNCACHE TABLE result")
    }
    
    def calculateTime(sql : String, funcName : String): Unit ={
      begin = System.nanoTime
      runSql(sql)
      runSql(sql)
      runSql(sql)
      end = System.nanoTime
      val dur_time = ((end - begin) / 1e9d) / 3.0
      val writer = new PrintWriter(outputPath + funcName + ".txt")
      writer.println("geomesa_" + funcName + "_time:" + dur_time)
      writer.close()
    }

//    def filePathMap(funcName : String): Unit={
//      funcName match {
//        case "st_point"
//      }{
//
//      }
//    }
    
    def TestSTPoint(): Unit ={
      val funcName = "st_point"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("x double, y double").load(filePath).cache()
      dataDF.createOrReplaceTempView("points")
      dataDF.show(20,0)
      val sql = "select ST_Point(x, y) from points"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    Geomesa doesn't have this function
//    def test_st_geomfromgeojson()

    def TestSTPointFromText(): Unit ={
      val funcName = "st_pointfromtext"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show(20,0)
      val sql = "select ST_PointFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTPolygonFromText(): Unit ={
      val funcName = "st_polygonfromtext"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show(20,0)
      val sql = "select ST_PolygonFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTAsText(): Unit ={
      val funcName = "st_astext"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show(20,0)
      val sql = "select ST_AsText(ST_GeomFromText(data)) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTPrecisionReduce   Geomesa has no this

    def TestSTLineStringFromText(): Unit ={
      val funcName = "st_linestringfromtext"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show(20,0)
      val sql = "select ST_LineFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeomFromWKT(): Unit ={
      val funcName = "st_geomfromwkt"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show(20,0)
      val sql = "select ST_GeomFromWKT(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeomFromText(): Unit ={
      val funcName = "st_geomfromtext"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("data string").load(filePath).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show(20,0)
      val sql = "select ST_GeomFromText(data) from data"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTIntersection  Geomesa has no this func

    def TestSTIsValid(): Unit ={
      val funcName = "st_isvalid"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("valid")
      dataDF.show(20,0)
      val sql = "select ST_IsValid(ST_GeomFromText(geos)) from valid"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTEquals(): Unit ={
      val funcName = "st_equals"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("equals")
      dataDF.show(20,0)
      val sql = "select ST_Equals(ST_GeomFromText(left), ST_GeomFromText(right)) from equals"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTTouches(): Unit ={
      val funcName = "st_touches"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("touches")
      dataDF.show(20,0)
      val sql = "select ST_Touches(ST_GeomFromText(left), ST_GeomFromText(right)) from touches"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTOverlaps(): Unit ={
      val funcName = "st_overlaps"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("overlaps")
      dataDF.show(20,0)
      val sql = "select ST_Overlaps(ST_GeomFromText(left), ST_GeomFromText(right)) from overlaps"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTCrosses(): Unit ={
      val funcName = "st_crosses"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("crosses")
      dataDF.show(20,0)
      val sql = "select ST_Crosses(ST_GeomFromText(left), ST_GeomFromText(right)) from crosses"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTIsSimple(): Unit ={
      val funcName = "st_issimple"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("simple")
      dataDF.show(20,0)
      val sql = "select ST_IsSimple(ST_GeomFromText(geos)) from simple"
     calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTGeometryType(): Unit ={
      val funcName = "st_geometry_type"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("geometry_type")
      dataDF.show(20,0)
      val sql = "select ST_GeometryType(ST_GeomFromText(geos)) from geometry_type"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTMakeValid()  Geomesa has no this func
//    def TestSTMakeValid(): Unit ={
//      var filePath = dataPath.concat("st_make_valid.csv")
//      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
//      dataDF.createOrReplaceTempView("make_valid")
//      dataDF.show(20,0)
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
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("contains")
      dataDF.show(20,0)
      val sql = "select ST_Contains(ST_GeomFromText(left), ST_GeomFromText(right)) from contains"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTIntersects(): Unit ={
      val funcName = "st_intersects"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("intersects")
      dataDF.show(20,0)
      val sql = "select ST_Intersects(ST_GeomFromText(left), ST_GeomFromText(right)) from intersects"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTWithIn(): Unit ={
      val funcName = "st_within"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("within")
      dataDF.show(20,0)
      val sql = "select ST_Within(ST_GeomFromText(left), ST_GeomFromText(right)) from within"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTDistance(): Unit ={
      val funcName = "st_distance"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("left string, right string").load(filePath).cache()
      dataDF.createOrReplaceTempView("distance")
      dataDF.show(20,0)
      val sql = "select ST_Distance(ST_GeomFromText(left), ST_GeomFromText(right)) from distance"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTArea(): Unit ={
      val funcName = "st_area"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("area")
      dataDF.show(20,0)
      val sql = "select ST_Area(ST_GeomFromText(geos)) from area"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTCentroid(): Unit ={
      val funcName = "st_centroid"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("centroid")
      dataDF.show(20,0)
      val sql = "select ST_Centroid(ST_GeomFromText(geos)) from centroid"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTLength(): Unit ={
      val funcName = "st_length"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("length")
      dataDF.show(20,0)
      val sql = "select ST_Length(ST_GeomFromText(geos)) from length"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTHausdorffDistance  Geometry has no this func

    def TestSTConvehull(): Unit ={
      val funcName = "st_test"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("convexhull")
      dataDF.show(20,0)
      val sql = "select ST_ConvexHull(ST_GeomFromText(geos)) from convexhull"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTNPoints(): Unit ={
      val funcName = "st_npoints"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("npoints")
      dataDF.show(20,0)
      val sql = "select ST_NumPoints(ST_GeomFromText(geos)) from npoints"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTEnvelope(): Unit ={
      val funcName = "st_envelope"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("envelope")
      dataDF.show(20,0)
      val sql = "select ST_Envelope(ST_GeomFromText(geos)) from envelope"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

    def TestSTBuffer(): Unit ={
      val funcName = "st_buffer"
      val filePath = dataPath + funcName + ".csv"
      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
      dataDF.createOrReplaceTempView("buffer")
      dataDF.show(20,0)
      val sql = "select ST_BufferPoint(ST_GeomFromText(geos), 1.2) from buffer"
      calculateTime(sql, funcName)
      dataDF.unpersist()
    }

//    TestSTUnionAggr()
//    TestSTEnvelopeAggr()

//    def TestSTTranslate(): Unit ={
//      val funcName = "st_transform"
//      val filePath = dataPath + funcName + ".csv"
//      val dataDF = spark.read.format("csv").option("header", false).option("delimiter", "|").schema("geos string").load(filePath).cache()
//      dataDF.createOrReplaceTempView("translate")
//      dataDF.show(20,0)
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
//      "Convehull"->(() =>         TestSTConvehull()),
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

