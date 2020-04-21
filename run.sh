spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Point -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f PointFromText -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Area -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f AsText -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Centroid -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Contains -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Crosses -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Distance -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Equals -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f GeometryType -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f GeomFromText -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f GeomFromWKT -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Intersects -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f IsSimple -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f IsValid -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Length -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f LineStringFromText -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f NPoints -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Touches -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Overlaps -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f WithIn -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f PolygonFromText -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Envelope -n 100 -t 5 -s false
spark-submit --master local[8] --jars ./dependencies/geomesa-spark-jts_2.11-2.1.1.jar,./dependencies/jts-core-1.14.0.jar,./dependencies/jts-io-1.14.0.jar,./dependencies/spatial4j-0.6.jar  --class TestGeomesa ./target/geomesa-spark-test-1.0-SNAPSHOT.jar -p /home/zc/tests/data/ -o /home/zc/tests/data/output/ -f Buffer -n 100 -t 5 -s false
