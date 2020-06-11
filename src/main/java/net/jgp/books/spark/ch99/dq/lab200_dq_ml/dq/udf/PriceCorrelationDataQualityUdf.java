package net.jgp.books.spark.ch99.dq.lab200_dq_ml.dq.udf;

import org.apache.spark.sql.api.java.UDF2;

import net.jgp.books.spark.ch99.dq.lab200_dq_ml.dq.service.PriceCorrelationDataQualityService;

public class PriceCorrelationDataQualityUdf
    implements UDF2<Double, Integer, Double> {

  private static final long serialVersionUID = 4949954702581973224L;

  public Double call(Double price, Integer guest) throws Exception {
    if (price == null || guest == null) {
      return -1.0;
    }
    return PriceCorrelationDataQualityService.checkPriceRange(price, guest);
  }

}
