package net.jgp.labs.spark.ch99.lab200_dq_ml.dq.service;

public abstract class PriceCorrelationDataQualityService {

	public static double checkPriceRange(double price, int guest) {
		if (guest < 14 && price > 90) {
			return -1;
		}
		return price;
	}

}
