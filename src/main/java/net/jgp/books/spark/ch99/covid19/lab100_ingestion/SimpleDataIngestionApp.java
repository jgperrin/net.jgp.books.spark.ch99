package net.jgp.books.spark.ch99.covid19.lab100_ingestion;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch99.covid19.x.utils.GitUtils;
import net.jgp.books.spark.ch99.x.utils.DataframeUtils;

/**
 * Cleans a dataset and then extrapolates date through machine learning, via
 * a linear regression using Apache Spark.
 * 
 * @author jgp
 *
 */
public class SimpleDataIngestionApp {
  private static Logger log =
      LoggerFactory.getLogger(SimpleDataIngestionApp.class);

  public static void main(String[] args) {
    SimpleDataIngestionApp app = new SimpleDataIngestionApp();
    app.start();
  }

  /**
   * Real work goes here...
   */
  private boolean start() {
    log.debug("-> start()");

    // Clone
    GitUtils.syncRepository(
        "https://github.com/CSSEGISandData/COVID-19.git",
        "./data/covid19-jhu");

    log.debug("##### Create Spark session");
    SparkSession spark = SparkSession.builder()
        .appName("Ingestion of Covid-19 data")
        .master("local[*]")
        .getOrCreate();


    log.debug("##### Ingestion");
    String filenames =
        "data/covid19-jhu/csse_covid_19_data/csse_covid_19_daily_reports/*.csv";
    Dataset<Row> df = spark
        .read()
        .format("csv")
        .option("inferSchema", true)
        .option("header", true)
        .load(filenames);
    
    // Stat
    log.debug("##### Stat");
    DataframeUtils.show(df);
    return true;
  }

}
