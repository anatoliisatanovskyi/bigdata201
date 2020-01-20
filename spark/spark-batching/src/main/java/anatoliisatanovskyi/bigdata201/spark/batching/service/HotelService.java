package anatoliisatanovskyi.bigdata201.spark.batching.service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.last;

import java.util.Arrays;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class HotelService {

	public Dataset<Row> getExpediaIncludingIdleDays(Dataset<Row> expedia) {
		WindowSpec window = Window.partitionBy("hotel_id").orderBy(col("srch_ci").desc()).rowsBetween(-1, 1);
		return expedia.select(col("srch_ci"), col("hotel_id"), last("srch_ci").over(window).as("prev_date"))
				.select(col("*"), datediff(col("srch_ci"), col("prev_date")).as("diff"));
	}

	public Dataset<Row> getIdleHotels(Dataset<Row> hotels, Dataset<Row> expediaIncludingIdleDays) {
		Dataset<Row> idleHotels = expediaIncludingIdleDays.filter("diff > 1 AND diff < 30");
		return hotels.select(col("Name"), col("Id")).join(idleHotels,
				hotels.col("Id").equalTo(idleHotels.col("hotel_id")));
	}

	public Dataset<Row> getValidHotels(Dataset<Row> hotels, Dataset<Row> idleHotels) {
		Dataset<Row> validData = hotels.join(idleHotels.select(col("hotel_id")),
				hotels.col("Id").equalTo(idleHotels.col("hotel_id")), "leftanti");
		return validData.select(col("Id"), col("Name"), col("Country"), col("City"), col("Address"), col("Latitude"),
				col("Longitude"));
	}

	public Dataset<Row> getExpediaWithValidHotels(Dataset<Row> expedia, Dataset<Row> validHotels) {
		return expedia.join(validHotels, expedia.col("hotel_id").equalTo(validHotels.col("Id")));
	}

	public Dataset<Row> groupByColumnCount(Dataset<Row> dataset, String... columns) {
		Column[] cols = Arrays.stream(columns).map(c -> col(c)).toArray(Column[]::new);
		return dataset.groupBy(cols).count().sort(cols);
	}
}
