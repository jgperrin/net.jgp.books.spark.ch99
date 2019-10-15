package net.jgp.labs.spark.ch99.lab200_dq_ml.dq.udf;

import org.apache.spark.sql.api.java.UDF1;

import net.jgp.labs.spark.ch99.lab200_dq_ml.dq.service.MinimumPriceDataQualityService;

public class MinimumPriceDataQualityUdf implements UDF1<Double, Double> {

  private static final long serialVersionUID = -201966159201746851L;

  public Double call(Double checkTotal) throws Exception {
    return MinimumPriceDataQualityService.checkMinimumPrice(checkTotal);
  }
}
