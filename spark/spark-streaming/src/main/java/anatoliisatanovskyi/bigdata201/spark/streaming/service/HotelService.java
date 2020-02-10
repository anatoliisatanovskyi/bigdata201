package anatoliisatanovskyi.bigdata201.spark.streaming.service;

import static org.apache.spark.sql.functions.*;

import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class HotelService {

	public Dataset<Row> enrichWithWeather(Dataset<Row> ds, Dataset<Row> hotelWeather) {
		Column joinCondition = ds.col("hotel_id").equalTo(hotelWeather.col("hotelId"))//
				.and(ds.col("srch_ci").equalTo(hotelWeather.col("wthr_date")));
		return ds.join(hotelWeather, joinCondition, "inner");
	}

	public Dataset<Row> filterTemperatureGreaterThen(Dataset<Row> ds, int maxTemperature) {
		return ds.filter(col("avg_tmpr_c").isNotNull().and(col("avg_tmpr_c").$greater(maxTemperature)));
	}

	public Dataset<Row> enrichWithStayDuration(Dataset<Row> ds) {
		return ds.withColumn("stay_duration", datediff(col("srch_co"), col("srch_ci")));
	}

	public Dataset<Row> enrichWithChildrenPresent(Dataset<Row> ds) {
		return ds.withColumn("children_present", col("srch_children_cnt").$greater(0));
	}

	public Dataset<Row> calculateCustomerPreferenceWithChildren(Dataset<Row> ds) {
		ds = ds.filter(col("children_present").equalTo(functions.lit(true)));
		return calculateCustomerPreference(ds);
	}

	public Dataset<Row> calculateCustomerPreferenceWithoutChildren(Dataset<Row> ds) {
		ds = ds.filter(col("children_present").equalTo(functions.lit(false)));
		return calculateCustomerPreference(ds);
	}

	public Dataset<Row> calculateCustomerPreference(Dataset<Row> ds) {

		Dataset<Row> enrichedWithType = ds.withColumn("stay_type", getTypeColumn(ds));

		Dataset<Row> counted = enrichedWithType.groupBy("hotel_id", "stay_type").count().as("t1");
		Dataset<Row> max = counted.groupBy("hotel_id").max("count").as("t2");

		Column joinCondition = counted.col("hotel_id").equalTo(max.col("hotel_id"))
				.and(counted.col("count").equalTo(max.col("max(count)")));

		return counted.join(max, joinCondition).select(col("t1.hotel_id"), col("t1.stay_type"));
	}

	public Dataset<Row> enrichWithRandomizedTimestamp(Dataset<Row> ds) {
		return ds.withColumn("timestamp", //
				when(rand().$greater(0.75), System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(30))//
						.when(rand().$greater(0.50), System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(20))//
						.when(rand().$greater(0.25), System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10))//
						.otherwise(System.currentTimeMillis()));
	}

	private Column getTypeColumn(Dataset<Row> ds) {
		Column duration = ds.col("stay_duration");
		return when(duration.between(15, 30), "long_stay")//
				.when(duration.between(8, 14), "standart_extended_stay")//
				.when(duration.between(2, 7), "standart_stay")//
				.when(duration.equalTo(1), "short_stay")//
				.otherwise("errorneous");
	}
}
