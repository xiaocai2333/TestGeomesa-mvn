import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import org.locationtech.geomesa.spark.jts._

object TestGeomesa {

  def main(args: Array[String]) {
    val data_path = "/home/zc/tests/data/10_2/"
    val spark = SparkSession.builder()
                  .appName("geomesa-spark profile")
                  .getOrCreate()
                  .withJTS

    import spark.implicits._

    var begin = System.nanoTime
    var end = System.nanoTime

    def TestSTPoint(): Unit ={
      val file_path = data_path.concat("st_point.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("x double, y double").load(file_path).cache()
      dataDF.createOrReplaceTempView("points")
      dataDF.show()
      val sql = "select ST_Point(x, y) from points"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_point_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    Geomesa doesn't have this function
//    def test_st_geomfromgeojson()

    def TestSTPointFromText(): Unit ={
      val file_path = data_path.concat("st_pointfromtext.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("data string").load(file_path).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show()
      val sql = "select ST_PointFromText(data) from data"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_pointfromtext_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTPolygonFromText(): Unit ={
      val file_path = data_path.concat("st_polygonfromtext.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("data string").load(file_path).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show()
      val sql = "select ST_PolygonFromText(data) from data"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_polygonfromtext_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTAsText(): Unit ={
      var file_path = data_path.concat("st_astext.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("data string").load(file_path).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show()
      val sql = "select ST_AsText(data) from data"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_astext_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    TestSTPrecisionReduce   Geomesa has no this

    def TestSTLineStringFromText(): Unit ={
      var file_path = data_path.concat("st_linestringfromtext.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("data string").load(file_path).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show()
      val sql = "select ST_LineFromText(data) from data"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_linestringfromtext_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTGeomFromWKT(): Unit ={
      var file_path = data_path.concat("st_geomfromwkt.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("data string").load(file_path).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show()
      val sql = "select ST_GeomFromWKT(data) from data"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_geomfromwkt_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTGeomFromText(): Unit ={
      var file_path = data_path.concat("st_geomfromtext.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("data string").load(file_path).cache()
      dataDF.createOrReplaceTempView("data")
      dataDF.show()
      val sql = "select ST_GeomFromText(data) from data"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_geomfromtext_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    TestSTIntersection  Geomesa has no this func

    def TestSTIsValid(): Unit ={
      var file_path = data_path.concat("st_isvalid.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("valid")
      dataDF.show()
      val sql = "select ST_IsValid(geos) from valid"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_isvalid_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTEquals(): Unit ={
      var file_path = data_path.concat("st_equals.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("equals")
      dataDF.show()
      val sql = "select ST_Equals(ST_GeomFromText(left), ST_GeomFromText(right)) from equals"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_equals_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTouches(): Unit ={
      var file_path = data_path.concat("st_touches.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("touches")
      dataDF.show()
      val sql = "select ST_Touches(ST_GeomFromText(left), ST_GeomFromText(right)) from touches"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_touches_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTOverlaps(): Unit ={
      var file_path = data_path.concat("st_overlaps.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("overlaps")
      dataDF.show()
      val sql = "select ST_Overlaps(ST_GeomFromText(left), ST_GeomFromText(right)) from overlaps"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_overlaps_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTCrosses(): Unit ={
      var file_path = data_path.concat("st_crosses.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("crosses")
      dataDF.show()
      val sql = "select ST_Crosses(ST_GeomFromText(left), ST_GeomFromText(right)) from crosses"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_crosses_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTIsSimple(): Unit ={
      var file_path = data_path.concat("st_issimple.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("simple")
      dataDF.show()
      val sql = "select ST_IsSimple(ST_GeomFromText(geos)) from simple"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_issimple_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTGeometryType(): Unit ={
      var file_path = data_path.concat("st_geometry_type.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("geometry_type")
      dataDF.show()
      val sql = "select ST_GeometryType(ST_GeomFromText(geos)) from geometry_type"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_geometry_type_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    TestSTMakeValid()  Geomesa has no this func
//    def TestSTMakeValid(): Unit ={
//      var file_path = data_path.concat("st_make_valid.csv")
//      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
//      dataDF.createOrReplaceTempView("make_valid")
//      dataDF.show()
//      val sql = "select ST_MakeValid(ST_GeomFromText(geos)) from make_valid"
//      begin = System.nanoTime
//      spark.sql(sql)
//      end = System.nanoTime
//      println("geomesa_st_make_valid_time: " + (end - begin) / 1e9d)
//      dataDF.unpersist()
//    }

//    TestSTSimplifyPreserveTopology    Geomesa has no this func
//    TestSTPolygonFromEnvelope Geomesa has no this func

    def TestSTContains(): Unit ={
      var file_path = data_path.concat("st_contains.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("contains")
      dataDF.show()
      val sql = "select ST_Contains(ST_GeomFromText(left), ST_GeomFromText(right)) from contains"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_contains_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTIntersects(): Unit ={
      var file_path = data_path.concat("st_intersects.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("intersects")
      dataDF.show()
      val sql = "select ST_Intersects(ST_GeomFromText(left), ST_GeomFromText(right)) from intersects"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_intersects_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTWithIn(): Unit ={
      var file_path = data_path.concat("st_within.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("within")
      dataDF.show()
      val sql = "select ST_Within(ST_GeomFromText(left), ST_GeomFromText(right)) from within"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_within_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTDistance(): Unit ={
      var file_path = data_path.concat("st_distance.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("left string, right string").load(file_path).cache()
      dataDF.createOrReplaceTempView("distance")
      dataDF.show()
      val sql = "select ST_Distance(ST_GeomFromText(left), ST_GeomFromText(right)) from distance"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_distance_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTArea(): Unit ={
      var file_path = data_path.concat("st_area.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("area")
      dataDF.show()
      val sql = "select ST_Area(ST_GeomFromText(geos)) from area"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_area_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTCentroid(): Unit ={
      var file_path = data_path.concat("st_centroid.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("centroid")
      dataDF.show()
      val sql = "select ST_Centroid(ST_GeomFromText(geos)) from centroid"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_centroid_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTLength(): Unit ={
      var file_path = data_path.concat("st_length.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("length")
      dataDF.show()
      val sql = "select ST_Length(ST_GeomFromText(geos)) from length"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_length_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    TestSTHausdorffDistance  Geometry has no this func

    def TestSTConvehull(): Unit ={
      var file_path = data_path.concat("st_convexhull.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("convexhull")
      dataDF.show()
      val sql = "select ST_Convexhull(ST_GeomFromText(geos)) from convexhull"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_convexhull_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTNPoints(): Unit ={
      var file_path = data_path.concat("st_npoints.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("npoints")
      dataDF.show()
      val sql = "select ST_NumPoints(ST_GeomFromText(geos)) from npoints"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_npoints_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTEnvelope(): Unit ={
      var file_path = data_path.concat("st_envelope.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("envelope")
      dataDF.show()
      val sql = "select ST_Envelope(ST_GeomFromText(geos)) from envelope"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_envelope_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

    def TestSTBuffer(): Unit ={
      var file_path = data_path.concat("st_buffer.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("buffer")
      dataDF.show()
      val sql = "select ST_BufferPoint(ST_GeomFromText(geos), 1.2) from buffer"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_buffer_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    TestSTUnionAggr()
//    TestSTEnvelopeAggr()

    def TestSTranslate(): Unit ={
      var file_path = data_path.concat("st_transform.csv")
      val dataDF = spark.read.format("csv").option("header", true).option("delimiter", ",").schema("geos string").load(file_path).cache()
      dataDF.createOrReplaceTempView("translate")
      dataDF.show()
      val sql = "select ST_Translate(ST_GeomFromText(geos), 'epsg:4326', 'epsg:3857') from translate"
      begin = System.nanoTime
      spark.sql(sql)
      spark.sql(sql)
      spark.sql(sql)
      end = System.nanoTime
      println("geomesa_st_translate_time: " + ((end - begin) / 1e9d) / 3.0)
      dataDF.unpersist()
    }

//    TestSTCurveToLine()

    TestSTPoint()
    TestSTPointFromText()
    TestSTArea()
    TestSTAsText()
    TestSTCentroid()
    TestSTContains()
    TestSTConvehull()
    TestSTCrosses()
    TestSTDistance()
    TestSTEquals()
    TestSTGeometryType()
    TestSTGeomFromText()
    TestSTGeomFromWKT()
    TestSTIntersects()
    TestSTIsSimple()
    TestSTIsValid()
    TestSTLength()
    TestSTLineStringFromText()
//    TestSTMakeValid()
    TestSTNPoints()
    TestSTouches()
    TestSTOverlaps()
    TestSTWithIn()
    TestSTPolygonFromText()
    TestSTWithIn()
    TestSTPointFromText()
    TestSTEnvelope()
    TestSTBuffer()
    TestSTranslate()


    // TODO: ST_Buffer, but there is st_bufferPoint udf in geomesa
  }
}

