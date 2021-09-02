package ai.jgp.books.spark.ch99.covid19.lab900_other_experiments;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvToDatasetApp {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System.getProperty("user.dir"));
    CsvToDatasetApp app = new CsvToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("CSV to Dataset")
        .master("local[*]")
        .getOrCreate();

    String filename = "data/covid19-jhu/csse_covid_19_data/csse_covid_19_daily_reports/04-02-2020.csv" ;
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", true)
        .option("header", true)
        .load(filename);
    df.show();
  }
}
