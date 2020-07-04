
package net.jgp.books.spark.ch99.covid19.lab300_day1_builder;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

    Dataset<Row> df = CovidIngester.ingest(spark);
    if (df == null) {
      log.error("Could not ingest data.");
      return false;
    }

    // Transformations
    df = DataPreparer.applyDataQualityRules(df);
    df = df
        .filter(col("country").equalTo("Italy"))
        .orderBy(col("reportedDate").asc_nulls_last());
    df.show(150, false);

    Dataset<Row> italyDf = df
        .filter(col("country").equalTo("Italy"))
        .groupBy("reportedDate")
        .agg(
            first("date").alias("date"),
            sum("confirmed").alias("confirmed"),
            sum("deaths").alias("deaths"),
            sum("recovered").alias("recovered"),
            sum("active").alias("active"),
            collect_list("ingester"))
        .orderBy(col("reportedDate").asc_nulls_last())
        .filter(col("confirmed").$greater$eq(5));
    Date minDate = (Date) italyDf.first().getAs("reportedDate");
    log.debug(
        "First day where confirmed cases where higher (or equal) than 5 cases: {}",
        minDate);
    italyDf = italyDf
        .withColumn("startDate", lit(minDate))
        .withColumn("day",
            datediff(col("reportedDate"), date_sub(col("startDate"), 1)));
    italyDf.show(200, false);

    // Stat
    log.debug("##### Stat");
    // DataframeUtils.show(df);
    // DataframeUtils.analyzeColumn(df, "state");
    // DataframeUtils.analyzeColumn(df, "country");
    return true;
  }

}
