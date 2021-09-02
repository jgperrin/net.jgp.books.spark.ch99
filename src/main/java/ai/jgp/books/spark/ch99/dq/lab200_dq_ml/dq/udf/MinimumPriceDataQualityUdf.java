package ai.jgp.books.spark.ch99.dq.lab200_dq_ml.dq.udf;

import org.apache.spark.sql.api.java.UDF1;

import ai.jgp.books.spark.ch99.dq.lab200_dq_ml.dq.service.MinimumPriceDataQualityService;

public class MinimumPriceDataQualityUdf implements UDF1<Double, Double> {

  private static final long serialVersionUID = -201966159201746851L;

  public Double call(Double checkTotal) throws Exception {
    return MinimumPriceDataQualityService.checkMinimumPrice(checkTotal);
  }
}
