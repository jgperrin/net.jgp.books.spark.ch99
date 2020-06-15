package net.jgp.books.spark.ch99.covid19.lab110_real_ingestion;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jgp.books.spark.ch99.covid19.x.utils.GitUtils;
import net.jgp.books.spark.ch99.x.utils.DataframeUtils;

/**
 * Cleans a dataset and then extrapolates date through machine learning, via
 * a linear regression using Apache Spark.
 * 
 * @author jgp
 *
 */
public class RealisticDataIngestionApp {
  private static Logger log =
      LoggerFactory.getLogger(RealisticDataIngestionApp.class);

  public static void main(String[] args) {
    RealisticDataIngestionApp app =
        new RealisticDataIngestionApp();
    app.start();
  }

  private SparkSession spark;

  /**
   * Real work goes here...
   */
  private boolean start() {
    log.debug("-> start()");
    String dataDirectory =
        "data/covid19-jhu/csse_covid_19_data/csse_covid_19_daily_reports";

    // Clone
    GitUtils.syncRepository(
        "https://github.com/CSSEGISandData/COVID-19.git",
        "./data/covid19-jhu");

    log.debug("##### Create Spark session");
    spark = SparkSession.builder()
        .appName("Ingestion of Covid-19 data")
        .master("local[*]")
        .getOrCreate();

    log.debug("##### List all files");
    DirectoryStream<Path> files;
    try {
      files = Files.newDirectoryStream(
          Paths.get(dataDirectory),
          path -> path.toString().endsWith(".csv"));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }

    Dataset<Row> df = null;
    for (Path p : files) {
      log.debug("####  Processing {}", p.getFileName());
      String header = null;
      try {
        header = Files.lines(p).findFirst().get();
      } catch (IOException e) {
        // handle exception.
      }
      Dataset<Row> intermediateDf = ingest(p.toString(), header);
      if (intermediateDf != null) {
        if (df == null) {
          df = intermediateDf;
        } else {
          df = df.unionByName(intermediateDf);
        }
      }
    }

    log.debug("##### Ingestion");
    String filenames =
        "data/covid19-jhu/csse_covid_19_data/csse_covid_19_daily_reports/01*.csv";

    // Stat
    log.debug("##### Stat");
    DataframeUtils.show(df);
    return true;
  }

  private Dataset<Row> ingest(String path, String header) {
    // TODO Auto-generated method stub
    Dataset<Row> df = null;
    switch (header) {
      case "Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered":
        log.debug("Using ingest 1 for [{}]", path);
        df = ingest1(path);
        break;

      case "FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key,Incidence_Rate,Case-Fatality_Ratio":
        log.debug("Using ingest 2 for [{}]", path);
        df = ingest2(path);
        break;

      case "FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key":
        log.debug("Using ingest 3 for [{}]", path);
        df = ingest3(path);
        break;

      default:
        log.error("Unknown ingester for {}", header);
        break;
    }
    return df;
  }

  /**
   * FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key
   * 
   * @param path
   * @return
   */
  private Dataset<Row> ingest3(String path) {
    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "fips",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "admin",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "state",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "country",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "lastUpdate",
            DataTypes.TimestampType,
            false),
        DataTypes.createStructField(
            "latitude",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "longitude",
            DataTypes.DoubleType,
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
            "combinedKey",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "incidenceRate",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "caseFatalityRatio",
            DataTypes.DoubleType,
            false) });

    Dataset<Row> df = spark
        .read()
        .format("csv")
        .schema(schema)
        .option("header", true)
        .load(path);
    return df;
  }

  /**
   * FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key,Incidence_Rate,Case-Fatality_Ratio
   * 
   * @param path
   * @return
   */
  private Dataset<Row> ingest2(String path) {
    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "fips",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "admin",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "state",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "country",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "lastUpdate",
            DataTypes.TimestampType,
            false),
        DataTypes.createStructField(
            "latitude",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "longitude",
            DataTypes.DoubleType,
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
            "combinedKey",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "incidenceRate",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "caseFatalityRatio",
            DataTypes.DoubleType,
            false) });

    Dataset<Row> df = spark
        .read()
        .format("csv")
        .schema(schema)
        .option("header", true)
        .load(path);
    return df;
  }

  private Dataset<Row> ingest1(String path) {
    // Creates the schema
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "state",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "country",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "lastUpdate",
            DataTypes.TimestampType,
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
            "combinedKey",
            DataTypes.StringType,
            false),
        DataTypes.createStructField(
            "incidenceRate",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "caseFatalityRatio",
            DataTypes.DoubleType,
            false) });

    Dataset<Row> df = spark
        .read()
        .format("csv")
        .schema(schema)
        .option("header", true)
        .load(path);

    df = df
        .withColumn("fips", lit(null))
        .withColumn("admin", lit(null))
        .withColumn("latitude", lit(null))
        .withColumn("longitude", lit(null));
    return df;
  }
}
