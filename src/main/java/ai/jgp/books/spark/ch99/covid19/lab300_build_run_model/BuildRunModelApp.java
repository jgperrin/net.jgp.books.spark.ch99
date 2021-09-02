package ai.jgp.books.spark.ch99.covid19.lab300_build_run_model;

import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans a dataset and then extrapolates date through machine learning, via
 * a linear regression using Apache Spark.
 * 
 * @author jgp
 *
 */
public class BuildRunModelApp {
  private static Logger log =
      LoggerFactory.getLogger(BuildRunModelApp.class);

  public static void main(String[] args) {
    BuildRunModelApp app = new BuildRunModelApp();
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
        .appName("Analysis & prediction based on Covid-19 data")
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

    // Phase 3
    // Clean the data
    // pure data -> analytics
    Dataset<Row> italyDf = DataAnalytics.buildCountryAggregate(df, "Italy");

    // build model
    GBTRegressionModel model = DataAnalytics.buildModel(spark, italyDf);

    // Some prediction
    DataAnalytics.predict(model, 200.0);
    DataAnalytics.predict(model, 300.0);
    DataAnalytics.predict(model, 400.0);
    DataAnalytics.predict(model, 500.0);

    // Dataset<Row> usaDf = DataAnalytics.buildCountryAggregate(df, "US");
    // usaDf = assembler.transform(usaDf);
    // usaDf.show(200, false);
    //
    // // Make predictions for USA
    // Dataset<Row> usaPredictionsDf = model.transform(usaDf);
    // evaluator = new RegressionEvaluator()
    // .setLabelCol("new")
    // .setPredictionCol("prediction")
    // .setMetricName("rmse");
    // rmse = evaluator.evaluate(usaPredictionsDf);
    // log.info("Root Mean Squared Error (RMSE) for the US: {}", rmse);
    //
    // DataAnalytics.predict(model, 200.0);

    // Stat
    // log.debug("##### Stat");
    // DataframeUtils.show(df);
    // DataframeUtils.analyzeColumn(df, "state");
    // DataframeUtils.analyzeColumn(df, "country");
    return true;
  }

}
