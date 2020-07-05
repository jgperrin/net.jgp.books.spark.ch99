
package net.jgp.books.spark.ch99.covid19.lab300_day1_builder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

import java.util.Date;

import javax.swing.text.StyledEditorKit.ItalicAction;

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

    // Needed by Spark v3.0.0
    spark.sql("set spark.sql.legacy.timeParserPolicy=CORRECTED");

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

    // Phase 3
    // Clean the data
    // pure data -> analytics
    Dataset<Row> italyDf = DataAnalytics.buildCountryAggregate(df, "Italy");
    italyDf.show(200, false);

//    Dataset<Row> usaDf = DataAnalytics.buildCountryAggregate(df, "US");
//    usaDf.show(200, false);
       

    // Stat
    log.debug("##### Stat");
    // DataframeUtils.show(df);
    // DataframeUtils.analyzeColumn(df, "state");
    // DataframeUtils.analyzeColumn(df, "country");
    return true;
  }

}
