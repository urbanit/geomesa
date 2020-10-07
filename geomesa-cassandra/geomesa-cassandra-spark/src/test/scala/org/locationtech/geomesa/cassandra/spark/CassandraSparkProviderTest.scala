/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.spark

import java.nio.file.{Files, Path}

import com.datastax.driver.core.{Cluster, SocketOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStore
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator, SparkSQLTestUtils}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class CassandraSparkProviderTest extends Specification with LazyLogging {

  sequential

  var storage: Path = _
  var dsParams: Map[String, String] = _
  var ds: CassandraDataStore = _

  var spark: SparkSession = _
  var sc: SparkContext = _
  var sql: SQLContext = _
  var sParams: Map[String, String] = _

  step {

    // initialize spark
    spark = org.locationtech.geomesa.spark.SparkSQLTestUtils.createSparkSession()
    sc = spark.sparkContext
    sql = spark.sqlContext

    //initialize cassandra
    storage = Files.createTempDirectory("cassandra")
    System.setProperty("cassandra.storagedir", storage.toString)
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 1200000L)
    var readTimeout: Int = SystemProperty("cassandraReadTimeout", "12000").get.toInt
    if (readTimeout < 0) {
      readTimeout = 12000
    }
    val host = EmbeddedCassandraServerHelper.getHost
    val port = EmbeddedCassandraServerHelper.getNativeTransportPort
    val cluster = new Cluster.Builder().addContactPoints(host).withPort(port)
        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout)).build().init()
    val session = cluster.connect()
    val cqlDataLoader = new CQLDataLoader(session)
    cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))

    var keyspace: String = "geomesa_cassandra";
    var catalog: String = "test_sft";

    // datastore
    dsParams = Map(
      "geomesa.cassandra.host" -> s"$host",
      Params.ContactPointParam.getName -> s"$host:$port",
      Params.KeySpaceParam.getName -> keyspace,
      Params.CatalogParam.getName -> catalog
    )
    ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[CassandraDataStore]

    // spark
    sParams =  Map(
      "geomesa.cassandra.host" -> s"$host",
      Params.KeySpaceParam.getName -> keyspace,
      "cassandra.catalog" -> catalog
    )

    //ingest data
    SparkSQLTestUtils.ingestChicago(ds)

    val df = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()

    logger.debug(df.schema.treeString)
    df.createOrReplaceTempView("chicago")
  }

  // Define feature schema
  lazy val chicagoSft =
    SimpleFeatureTypes.createType("chicago",
      "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")

  // Make test features
  lazy val chicagoFeatures: Seq[SimpleFeature] = Seq(
    ScalaSimpleFeature.create(chicagoSft, "1", "true", 1, "2016-01-01T00:00:00.000Z", "POINT (-76.5 38.5)"),
    ScalaSimpleFeature.create(chicagoSft, "2", "true", 2, "2016-01-02T00:00:00.000Z", "POINT (-77.0 38.0)"),
    ScalaSimpleFeature.create(chicagoSft, "3", "true", 3, "2016-01-03T00:00:00.000Z", "POINT (-78.0 39.0)"),
    ScalaSimpleFeature.create(chicagoSft, "4", "true", 4, "2016-01-01T00:00:00.000Z", "POINT (-73.5 39.5)"),
    ScalaSimpleFeature.create(chicagoSft, "5", "true", 5, "2016-01-02T00:00:00.000Z", "POINT (-74.0 35.5)"),
    ScalaSimpleFeature.create(chicagoSft, "6", "true", 6, "2016-01-03T00:00:00.000Z", "POINT (-79.0 37.5)")
  )

  "The CassandraSpatialRDDProvider" should {
    "read from the embedded Cassandra database" in {
      // val ds = DataStoreFinder.getDataStore(params)
      ds.createSchema(chicagoSft)

      WithClose(ds.getFeatureWriterAppend("chicago", Transaction.AUTO_COMMIT)) { writer =>
        chicagoFeatures.take(3).foreach(FeatureUtils.write(writer, _, useProvidedFid = true))
      }

      val rdd = GeoMesaSpark(sParams).rdd(new Configuration(), sc, dsParams, new Query("chicago"))
      rdd.count() mustEqual 3l
    }

     "write to the embedded Cassandra database" in {
       val writeRdd = sc.parallelize(chicagoFeatures)
       GeoMesaSpark(sParams).save(writeRdd, dsParams, "chicago")
       val readRdd = GeoMesaSpark(sParams).rdd(new Configuration(), sc, dsParams, new Query("chicago"))
       readRdd.count() mustEqual 6l
     }

      "select by secondary indexed attribute" >> {
         
            val dfi = spark.read
                      .format("geomesa")
                      .options(dsParams)
                      .option("geomesa.feature", "chicago")
                      .load()

        val cases = dfi.select("case_number").where("case_number = 1").collect().map(_.getInt(0))
        cases.length mustEqual 1
      }

     "complex st_buffer" >> {
        val buf = sql.sql("select st_asText(st_bufferPoint(geom,10)) from chicago where case_number = 1").collect().head.getString(0)
        sql.sql(
          s"""
             |select *
             |from chicago
             |where
             |  st_contains(st_geomFromWKT('$buf'), geom)
           """.stripMargin
        ).collect().length must beEqualTo(1)
     }
  }
}
