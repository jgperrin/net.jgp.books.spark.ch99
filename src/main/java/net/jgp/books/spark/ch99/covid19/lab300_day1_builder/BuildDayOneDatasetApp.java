
package net.jgp.books.spark.ch99.covid19.lab300_day1_builder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch99.x.utils.DataframeUtils;

/**
 * Cleans a dataset and then extrapolates date through machine learning, via
 * a linear regression using Apache Spark.
 * 
 * @author jgp
 *
 */
public class BuildDayOneDatasetApp {
  private static Logger log =
      LoggerFactory.getLogger(BuildDayOneDatasetApp.class);

  public static void main(String[] args) {
    BuildDayOneDatasetApp app = new BuildDayOneDatasetApp();
    app.start();
  }

  private SparkSession spark;

  /**
   * Real work goes here...
   */
  private boolean start() {
    log.debug("-> start()");

    log.debug("##### Create Spark session");
    spark = SparkSession.builder()
        .appName("Ingestion of Covid-19 data")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = CovidIngester.ingest(spark);
    if (df == null) {
      log.error("Could not ingest data.");
      return false;
    }

    // Transformations
    df = DataPreparer.applyDataQualityRules(df);
    
    // Stat
    log.debug("##### Stat");
    DataframeUtils.show(df);
    DataframeUtils.analyzeColumn(df, "state");
    DataframeUtils.analyzeColumn(df, "country");
    return true;
  }

}
