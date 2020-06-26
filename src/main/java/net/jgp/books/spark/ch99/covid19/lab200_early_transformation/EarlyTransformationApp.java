
package net.jgp.books.spark.ch99.covid19.lab200_early_transformation;

import static org.apache.spark.sql.functions.*;

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
public class EarlyTransformationApp {
  private static Logger log =
      LoggerFactory.getLogger(EarlyTransformationApp.class);

  public static void main(String[] args) {
    EarlyTransformationApp app = new EarlyTransformationApp();
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
    df = df
        .repartition(1)
        //.filter(df.col("country").contains("Korea"))
        .withColumn("country",
            when(
                df.col("country").equalTo("Republic of Korea").or(df.col("country").equalTo("Korea, South")),
                lit("South Korea"))
                    .otherwise(df.col("country")));
    df = df
        .withColumn("country",
            when(
                df.col("country").contains("China"),
                lit("China"))
                    .otherwise(df.col("country")));
    df = df
        .withColumn("combinedKey",
            when(
                df.col("combinedKey").isNull(),
                concat_ws(", ", df.col("state"), df.col("country")))
                    .otherwise(df.col("combinedKey")))
        .withColumn("date", to_date(df.col("lastUpdate")));
    
    // Stat
    log.debug("##### Stat");
    DataframeUtils.show(df);
    DataframeUtils.analyzeColumn(df, "state");
    DataframeUtils.analyzeColumn(df, "country");
    return true;
  }

}
