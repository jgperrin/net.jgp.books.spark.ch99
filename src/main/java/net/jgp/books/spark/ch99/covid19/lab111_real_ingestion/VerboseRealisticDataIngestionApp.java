package net.jgp.books.spark.ch99.covid19.lab111_real_ingestion;

import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
 * This version is the same as lab #110, with additional verbosity and
 * comments.
 * 
 * @author jgp
 *
 */
public class VerboseRealisticDataIngestionApp {
  private static Logger log =
      LoggerFactory.getLogger(VerboseRealisticDataIngestionApp.class);

  public static void main(String[] args) {
    VerboseRealisticDataIngestionApp app =
        new VerboseRealisticDataIngestionApp();
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
    DirectoryStream<Path> directory;
    try {
      directory = Files.newDirectoryStream(
          Paths.get(dataDirectory),
          path -> path.toString().toLowerCase().endsWith(".csv"));
    } catch (IOException e) {
      log.error(
          "Could not list files from directory {}, got {}.",
          dataDirectory, e.getMessage());
      return false;
    }

    // Sorting is only used for debugging, it does not affect outcome.
    log.debug("##### Sort files");
    List<Path> files = new ArrayList<>();
    directory.forEach(files::add);
    files.sort(Comparator.comparing(Path::toString));

    Dataset<Row> df = null;
    for (Path p : files) {
      log.debug("####  Processing {}", p.getFileName());
      String header = null;
      try {
        header = Files.lines(p).findFirst().get();
        log.trace(
            "###   First and last header character are [{}]..[{}]",
            (int) header.charAt(0),
            header.charAt(header.length() - 1));
      } catch (IOException e) {
        log.error("Error whilte reading {}, got {}.", p, e.getMessage());
      }

      // Remove Unicode Character 'ZERO WIDTH NO-BREAK SPACE' (U+FEFF)
      if (header.charAt(0) == 65279) {
        log.debug("{} starts with a ZERO WIDTH NO-BREAK SPACE",
            p.getFileName());
        header = header.substring(1);
      }
      Dataset<Row> intermediateDf = ingest(p.toString(), header);
      if (intermediateDf != null) {
        if (log.isDebugEnabled()) {
          log.debug("{} contains {} records", p.getFileName(),
              intermediateDf.count());
        }
        if (df == null) {
          df = intermediateDf;
        } else {
          df = df.unionByName(intermediateDf);
        }
      }
    }

    // df = df.filter(df.col("country").equalTo("US"));

    // Stat
    log.debug("##### Stat");
    DataframeUtils.show(df);
    return true;
  }

  private Dataset<Row> ingest(String path, String header) {
    Dataset<Row> df = null;
    switch (header) {
      case "Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered":
        log.debug("Using ingest 1 for [{}], header length is [{}]", path,
            header.length());
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

      case "Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude":
        log.debug("Using ingest 4 for [{}]", path);
        df = ingest4(path);
        break;

      default:
        log.error("Unknown ingester for [{}], header length is [{}]",
            header, header.length());
        break;
    }
    return df;
  }

  /**
   * Province/State,Country/Region,Last
   * Update,Confirmed,Deaths,Recovered,Latitude,Longitude
   * 
   * @param path
   * @return
   */
  private Dataset<Row> ingest4(String path) {
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
            "latitude",
            DataTypes.DoubleType,
            false),
        DataTypes.createStructField(
            "longitude",
            DataTypes.DoubleType,
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
            false),
        DataTypes.createStructField(
            "fips",
            DataTypes.IntegerType,
            false),
        DataTypes.createStructField(
            "admin",
            DataTypes.StringType,
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
