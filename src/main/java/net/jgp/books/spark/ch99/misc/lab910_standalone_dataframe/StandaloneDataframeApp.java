package net.jgp.books.spark.ch99.misc.lab910_standalone_dataframe;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * .
 * 
 * @author jgp
 *
 */
public class StandaloneDataframeApp {

  public static void main(String[] args) {
    StandaloneDataframeApp app = new StandaloneDataframeApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Standalone dataframe")
        // .master("spark://172.31.35.171:7077")
        .getOrCreate();

    String[] l = new String[] { "a", "b", "c", "d" };
    List<String> data = Arrays.asList(l);
    Dataset<String> ds = spark.createDataset(data, Encoders.STRING());
    Dataset<Row> df = ds.toDF();
    df.show();
  }
}
