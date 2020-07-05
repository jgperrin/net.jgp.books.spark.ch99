package net.jgp.books.spark.ch99.covid19.lab300_day1_builder;

import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.when;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class DataPurifier {

  public static Dataset<Row> applyDataQualityRules(Dataset<Row> df) {
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

    return df;
  }

}
