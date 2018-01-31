package com.reusable.functions.Analytics

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

/**
 * This object contains functions which can be invoked where ever applicable.
 *
 * @author  Varanasi Venkata Gowri Sai Rakesh Kumar Varanasi
 * @version 1.0
 * @since   30-OCT-2017
 */
object Utils {
  
  /**
   * This Function finds Consecutive Date Range for given date column and for a Group By list of columns
   *
   * @author  Varanasi Venkata Gowri Sai Rakesh Kumar Varanasi
   * @version 1.0
   * @since   01-NOV-2017
   */
  def getDateRange(df: DataFrame, dateColumn: String, groupList: List[String], sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    // This is the constant as in on which column to perform Rank
    val dateColumnList = List(dateColumn)

    // Apply Map on the column
    val dateColumnName = dateColumnList.map(name => col(name))

    //Apply Map on the columns for partition by clasue in Window Section 
    val partitionByColumnNames = groupList.map(name => col(name))

    // is the list of columns which are key for grouping the dataset 
    val AllColumnNames = groupList :+ "TEMP_GROUP"

    // Apply Map on Column Names
    val groupByColumnNames = AllColumnNames.map(name => col(name))

    // Rank the date column and subtract it from given date
    val date_grp_df = df.withColumn("SL_NO", row_number.over(Window.partitionBy(partitionByColumnNames: _*).orderBy(dateColumnName: _*)))
      .withColumn("TEMP_GROUP", expr("date_sub(" + dateColumnName(0) + ",SL_NO)"))

    // Aggregate the data to find Min Start Date and Max End Date to get the consecutive date range  
    val date_range_df = date_grp_df.groupBy(groupByColumnNames: _*)
      .agg(min(dateColumnName(0)).as("START_DATE"), max(dateColumnName(0)).as("END_DATE"), (count($"TEMP_GROUP").cast(IntegerType)).as("RANGE_COUNT"))
      .drop("TEMP_GROUP")

    // Finally Return the DataFrame  
    date_range_df

  }

}