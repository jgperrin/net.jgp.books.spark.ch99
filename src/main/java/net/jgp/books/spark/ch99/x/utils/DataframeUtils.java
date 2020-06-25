package net.jgp.books.spark.ch99.x.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public abstract class DataframeUtils {

  public static void show(Dataset<Row> df) {
    df.sample(1).show(100, false);
    df.printSchema();
    System.out.println(df.rdd().getNumPartitions() + " partition(s).");
    System.out.println(df.count() + " record(s).");
  }

  public static void analyzeColumn(Dataset<Row> df, String col) {
    Dataset<Row> workDf = df.groupBy(col).agg(count(col).as(K.COUNT)).orderBy(col(K.COUNT).desc());
    workDf.show();
    workDf.orderBy(col(K.COUNT).asc()).show(5);
    workDf.printSchema();
  }

}
