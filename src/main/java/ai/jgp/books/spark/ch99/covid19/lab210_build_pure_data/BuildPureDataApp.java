package ai.jgp.books.spark.ch99.covid19.lab210_build_pure_data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.jgp.books.spark.ch99.x.utils.DataframeUtils;

/**
 * Cleans a dataset and then extrapolates date through machine learning, via
 * a linear regression using Apache Spark.
 * 
 * @author jgp
 *
 */
public class BuildPureDataApp {
  private static Logger log =
      LoggerFactory.getLogger(BuildPureDataApp.class);

  public static void main(String[] args) {
    BuildPureDataApp app = new BuildPureDataApp();
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
        .appName("Build pure data")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .master("local[*]")
        .getOrCreate();

    // Phase 1
    // Ingest the data
    // (files) -> raw data
    Dataset<Row> df = CovidIngester.ingest(spark);
    if (df == null) {
      log.error("Could not ingest data.");
      return false;
    }

    // Phase 2
    // Clean the data
    // raw data -> pure data
    df = DataPurifier.applyDataQualityRules(df);

    // Stat
    log.debug("##### Stat");
    DataframeUtils.show(df);
    // DataframeUtils.analyzeColumn(df, "state");
    // DataframeUtils.analyzeColumn(df, "country");
    return true;
  }

}
