package ai.jgp.books.spark.ch99.covid19.lab210_build_pure_data;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.jgp.books.spark.ch99.covid19.x.utils.GitUtils;

public abstract class CovidIngester {
  private static Logger log = LoggerFactory.getLogger(CovidIngester.class);

  private static SparkSession spark;

  /**
   * Ingest the whole covid data
   * 
   * @return
   */
  public static Dataset<Row> ingest(SparkSession spark) {
    CovidIngester.spark = spark;

    String dataDirectory =
        "data/covid19-jhu/csse_covid_19_data/csse_covid_19_daily_reports";

    // Clone
    GitUtils.syncRepository(
        "https://github.com/CSSEGISandData/COVID-19.git",
        "./data/covid19-jhu");

    log.debug("##### List all files");
    DirectoryStream<Path> files;
    try {
      files = Files.newDirectoryStream(
          Paths.get(dataDirectory),
          path -> path.toString().endsWith(".csv"));
    } catch (IOException e) {
      log.error(
          "Could not list files from directory {}, got {}.",
          dataDirectory, e.getMessage());
      return null;
    }

    Dataset<Row> df = null;
    for (Path p : files) {
      log.trace("####  Processing {}", p.getFileName());
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
        header = header.substring(1);
      }
      Dataset<Row> intermediateDf =
          mainIngest(p.toString(), header, p.getFileName());
      if (intermediateDf != null) {
        if (df == null) {
          df = intermediateDf;
        } else {
          df = df.unionByName(intermediateDf);
        }
      }
    }

    return df;
  }

  private static Dataset<Row> mainIngest(
      String path, String header, Path filename) {
    Dataset<Row> df = null;
    switch (header) {

      // Early January
      case "Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered":
        log.trace("Using ingest 1 for [{}], header length is [{}]", path,
            header.length());
        df = ingester1(path);
        df = df.withColumn("ingester", lit(1));
        break;

      // February
      case "Province/State,Country/Region,Last Update,Confirmed,Deaths,Recovered,Latitude,Longitude":
        log.trace("Using ingest 2 for [{}]", path);
        df = ingester2(path);
        df = df.withColumn("ingester", lit(2));
        break;

      // March
      case "FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key":
        log.trace("Using ingest 3 for [{}]", path);
        df = ingester3(path);
        df = df.withColumn("ingester", lit(3));
        break;

      // April and later
      case "FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key,Incidence_Rate,Case-Fatality_Ratio":
        log.trace("Using ingest 4 for [{}]", path);
        df = ingester4(path);
        df = df.withColumn("ingester", lit(4));
        break;

      default:
        log.error("Unknown ingester for [{}], header length is [{}]",
            header, header.length());
        break;
    }
    df = df.withColumn("reportedDate",
        to_date(lit(filename.toString().substring(0, 10)), "MM-dd-yyyy"));
    return df;
  }

  private static Date filenameToDate(String filename) {
    try {
      return new SimpleDateFormat("MM-dd-yyyy")
          .parse(filename.substring(0, 9));
    } catch (ParseException e) {
      return null;
    }
  }

  /**
   * Province/State,Country/Region,Last
   * Update,Confirmed,Deaths,Recovered,Latitude,Longitude
   * 
   * @param path
   * @return
   */
  private static Dataset<Row> ingester2(String path) {
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
  private static Dataset<Row> ingester3(String path) {
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
        .option("timestampFormat", "M/d/y H:m")
        .load(path);
    return df;
  }

  /**
   * FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key,Incidence_Rate,Case-Fatality_Ratio
   * 
   * @param path
   * @return
   */
  private static Dataset<Row> ingester4(String path) {
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

  private static Dataset<Row> ingester1(String path) {
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
        .option("timestampFormat", "M/d/y H:m")
        .load(path);

    df = df
        .withColumn("fips", lit(null))
        .withColumn("admin", lit(null))
        .withColumn("latitude", lit(null))
        .withColumn("longitude", lit(null));
    return df;
  }
}
