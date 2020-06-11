package net.jgp.books.spark.ch99.covid19.lab100_ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch99.covid19.x.utils.GitUtils;

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
    SimpleDataIngestionApp app =
        new SimpleDataIngestionApp();
    app.start();
  }

  /**
   * Real work goes here...
   */
  private void start() {
    log.debug("-> start()");

    // Clone
    GitUtils.syncRepository(
        "https://github.com/CSSEGISandData/COVID-19.git",
        "./data/covid19-jhu");

    // SparkSession spark = SparkSession.builder()
    // .appName("DQ4ML")
    // .master("local[*]")
    // .getOrCreate();
    //
    // // DQ Section
    // // ----------
    //
    // spark.udf().register("minimumPriceRule",
    // new MinimumPriceDataQualityUdf(), DataTypes.DoubleType);
    // spark.udf().register("priceCorrelationRule",
    // new PriceCorrelationDataQualityUdf(), DataTypes.DoubleType);
    //
    // // Load our dataset
    // String filename = "data/restaurant/checks.csv";
    // Dataset<Row> df = spark
    // .read()
    // .format("csv")
    // .option("inferSchema", true)
    // .option("header", false)
    // .load(filename);
    //
    // // simple renaming of the columns
    // df = df.withColumnRenamed("_c0", "guest");
    // df = df.withColumnRenamed("_c1", "price");
    //
    // System.out.println("----");
    // System.out.println("Load & Format");
    // df.show();
    // System.out.println("----");
    //
    // // apply DQ rules
    // // 1) min price
    // df = df.withColumn("price_no_min",
    // callUDF("minimumPriceRule", df.col("price")));
    // System.out.println("----");
    // System.out.println("1st DQ rule");
    // df.printSchema();
    // df.show(50);
    // System.out.println("----");
    //
    // df.createOrReplaceTempView("price");
    // df = spark.sql(
    // "SELECT cast(guest as int) guest, price_no_min AS price FROM price
    // WHERE price_no_min > 0");
    // System.out.println("----");
    // System.out.println("1st DQ rule - clean-up");
    // df.printSchema();
    // df.show(50);
    // System.out.println("----");
    //
    // // 2) correlated price
    // df = df.withColumn("price_correct_correl",
    // callUDF("priceCorrelationRule", df.col("price"), df.col("guest")));
    // df.createOrReplaceTempView("price");
    // df = spark.sql(
    // "SELECT guest, price_correct_correl AS price FROM price WHERE
    // price_correct_correl > 0");
    //
    // System.out.println("----");
    // System.out.println("2nd DQ rule");
    // df.show(50);
    // System.out.println("----");
    //
    // // ML Section
    // // ----------
    //
    // // Creates the "label" column, required by the LR algorithm.
    // df = df.withColumn("label", df.col("price"));
    //
    // // Puts all the columns that will be part of the feature in an array,
    // so
    // // you can assemble them later. Here we have only one column in our
    // // feature.
    // String[] inputCols = new String[1];
    // inputCols[0] = "guest";
    //
    // // Assembles the features in one column called "features".
    // VectorAssembler assembler = new VectorAssembler()
    // .setInputCols(inputCols)
    // .setOutputCol("features");
    // df = assembler.transform(df);
    // df.printSchema();
    // df.show();
    //
    // // Lots of complex ML code goes here (just kidding...)
    //
    // // Build the linear regression
    // LinearRegression lr = new LinearRegression()
    // .setMaxIter(40)
    // .setRegParam(1)
    // .setElasticNetParam(1);
    //
    // // Fit the model to the data
    // LinearRegressionModel model = lr.fit(df);
    //
    // // Given a dataset, predict each point's label, and show the results.
    // model.transform(df).show();
    //
    // // Mostly debug and info-to-look-smart
    // LinearRegressionTrainingSummary trainingSummary = model.summary();
    // System.out
    // .println("numIterations: " + trainingSummary.totalIterations());
    // System.out.println("objectiveHistory: "
    // + Vectors.dense(trainingSummary.objectiveHistory()));
    // trainingSummary.residuals().show();
    // System.out.println("RMSE: " +
    // trainingSummary.rootMeanSquaredError());
    // System.out.println("r2: " + trainingSummary.r2());
    //
    // double intersect = model.intercept();
    // System.out.println("Intersection: " + intersect);
    // double regParam = model.getRegParam();
    // System.out.println("Regression parameter: " + regParam);
    // double tol = model.getTol();
    // System.out.println("Tol: " + tol);
    //
    // // Prediction code
    // Double feature = 40.0;
    // Vector features = Vectors.dense(40.0);
    // double p = model.predict(features);
    //
    // // Catering business outcome for 40 guests
    // System.out.println("Prediction for " + feature + " guests is " + p);
  }
}
