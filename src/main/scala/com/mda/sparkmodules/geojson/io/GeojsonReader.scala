package com.mda.sparkmodules.geojson.io

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

object GeojsonReader {
  def loadGeojsonFile(fileName : String): DataFrame = {
    //get spark context
    val sparkContext = SparkContext.getOrCreate()
    //get sql context
    val sqlContext = new SQLContext(sparkContext)
    //transform json to dataframe
    return sqlContext.read.option("multiline", true).option("mode", "PERMISSIVE").json(fileName)
  }
}
