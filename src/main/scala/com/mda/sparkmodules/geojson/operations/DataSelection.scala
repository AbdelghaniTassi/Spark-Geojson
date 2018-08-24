package com.mda.sparkmodules.geojson.operations

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataSelection(var geojsonDataFrame : DataFrame) {

  def selectGeometries(): DataFrame = {
    //select only geometry col and remove the first lines having null value
    val features = geojsonDataFrame.select(org.apache.spark.sql.functions.explode(org.apache.spark.sql.functions.col("features")).alias("feature")).select("feature.type", "feature.geometry")
    features.createOrReplaceTempView("features")

    //get spark context
    val sparkContext = SparkContext.getOrCreate()
    //get sql context
    val sqlContext = new SQLContext(sparkContext)

    //select coordinates and geometry type
    return sqlContext.sql("select geometry.type, geometry.coordinates FROM features")

  }

  def filterFeaturesByPropeties(propertyName : String, propertyValue : String ): DataFrame = {
    //return geojsonDataFrame.
    return geojsonDataFrame
  }

  def filterFeaturesByBoundingBox(): DataFrame = {
    return geojsonDataFrame
  }

  def groupFeaturesByProperty(): DataFrame = {
    return geojsonDataFrame
  }



}
