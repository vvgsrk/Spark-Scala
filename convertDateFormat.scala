package com.reusable.functions.Analytics

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window


/**
 * This object contains functions which can be invoked in Pricing Exception Requirements
 *
 * @author  Varanasi Venkata Gowri Sai Rakesh Kumar Varanasi
 * @version 1.0
 * @since   11-FEB-2018
 */
object Utils {  
  
  /**
   * This function Converts Date Column format (YYYY-MM-DD) to a given format
   *
   * @author  Varanasi Venkata Gowri Sai Rakesh Kumar Varanasi
   * @version 1.0
   * @since   11-FEB-2018
   */
  def convertDateFormat(df: DataFrame, dateColList: List[String], dateformat: String): DataFrame = {
    
    // Get All Column Names from the given data frame
    val all_cols_list = df.columns.toList
    
    // Find the diffrence between lists 
    val diff_list = all_cols_list diff dateColList
    
    // Apply Map
    val diff_cols = diff_list.map(name => col(name))
    
    // Apply date_format Function on each column
    val dateColumnNames = dateColList.map(name => date_format(col(name), dateformat).as(name))       
    
    // Finally Combine the list's
    val full_list = diff_cols ::: dateColumnNames
    
    // Select all columns
    val result_df = df.select(full_list: _*)
    
    // Return it
    result_df    
  } 
  
}