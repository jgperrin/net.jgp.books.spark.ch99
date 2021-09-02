package ai.jgp.books.spark.ch99.covid19.lab300_build_run_model;

import static java.lang.Math.toIntExact;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.date_sub;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataAnalytics {
  private static Logger log =
      LoggerFactory.getLogger(DataAnalytics.class);

  public static Dataset<Row> buildCountryAggregate(Dataset<Row> df,
      String country) {
    Dataset<Row> aggregateDf = df
        .filter(col("country").equalTo(country))
        .groupBy("reportedDate")
        .agg(
            first("date").alias("date"),
            sum("confirmed").alias("confirmed"),
            sum("deaths").alias("deaths"),
            sum("recovered").alias("recovered"),
            sum("active").alias("active"),
            // adds a column with the ingester id, useful for debugging
            collect_list("ingester").alias("ingester"))
        .orderBy(col("reportedDate").asc_nulls_last())
        .filter(col("confirmed").$greater$eq(5));
    Date minDate = (Date) aggregateDf.first().getAs("reportedDate");
    log.debug(
        "First day where confirmed cases where higher (or equal) than 5 cases: {}",
        minDate);
    aggregateDf = aggregateDf
        .withColumn("startDate", lit(minDate))
        .withColumn("day",
            datediff(col("reportedDate"), date_sub(col("startDate"), 1)))
        .drop("ingester")
        .drop("startDate");
    // df.withColumn("diff_Amt_With_Prev_Month", $"Amount" -
    // when((lag("Amount", 1).over(windowSpec)).isNull,
    // 0).otherwise(lag("Amount", 1).over(windowSpec)))
    WindowSpec win = Window.orderBy("day");
    aggregateDf = aggregateDf.withColumn(
        "new",
        col("confirmed")
            .$minus(when((lag("confirmed", 1).over(win)).isNull(), 0)
                .otherwise(lag("confirmed", 1).over(win))));
    return aggregateDf;
  }

  public static GBTRegressionModel buildModel(SparkSession spark,
      Dataset<Row> df) {
    int maxDay = toIntExact(df.count());

    Dataset<Row> df2 = buildGoalDataframe(spark, maxDay);
    df = df.unionByName(df2);
    String[] inputCols = new String[1];
    inputCols[0] = "day";
    VectorAssembler assembler = new VectorAssembler()
        .setInputCols(inputCols)
        .setOutputCol("features");
    df = assembler.transform(df);
    df.show(200, false);

    // Start a GBTRegressor
    GBTRegressor gbt = new GBTRegressor()
        .setLabelCol("new")
        .setFeaturesCol("features")
        .setMaxIter(150)
        .setLossType("absolute")
        .setFeatureSubsetStrategy("all");

    // Train model
    GBTRegressionModel model = gbt.fit(df);

    // Measures quality index for training data
    Dataset<Row> predictionsDf = model.transform(df);
    RegressionEvaluator evaluator = new RegressionEvaluator()
        .setLabelCol("new")
        .setPredictionCol("prediction")
        .setMetricName("rmse");
    double rmse = evaluator.evaluate(predictionsDf);
    log.debug("Root Mean Squared Error (RMSE): {}", rmse);
    log.debug("Learned regression GBT model:\n{}", model.toDebugString());

    return model;
  }

  public static Dataset<Row> buildGoalDataframe(SparkSession spark,
      int maxDay) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "reportedDate",
            DataTypes.DateType,
            false),
        DataTypes.createStructField(
            "date",
            DataTypes.DateType,
            false),
        DataTypes.createStructField(
            "confirmed",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "deaths",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "recovered",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "active",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "day",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "new",
            DataTypes.IntegerType,
            false) });

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      rows.add(RowFactory.create(null, null, 0, 0, 0, 0, maxDay + i +365, 0));
    }

    return spark.createDataFrame(rows, schema);
  }

  public static void predict(GBTRegressionModel model, double feature) {
    double p = model.predict(Vectors.dense(feature));
    log.info("New cases for day #{}: {}", feature, p);
  }

}
