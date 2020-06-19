package net.jgp.books.spark.ch99.x.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class DataframeUtils {

  public static void show(Dataset<Row> df) {
    df.show(100, false);
    df.printSchema();
    System.out.println(df.count() + " records.");
  }

}
