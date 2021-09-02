package ai.jgp.books.spark.ch99.dq.lab200_dq_ml.dq.service;

public abstract class MinimumPriceDataQualityService {

  private static final int MIN_PRICE = 20;

  public static double checkMinimumPrice(double price) {
    if (price < MIN_PRICE) {
      return -1;
    } else {
      return price;
    }
  }
}
