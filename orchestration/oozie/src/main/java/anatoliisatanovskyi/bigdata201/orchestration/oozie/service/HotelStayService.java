package anatoliisatanovskyi.bigdata201.orchestration.oozie.service;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HotelStayService {
	
	public Dataset<Row> getHotelStayDuration(Dataset<Row> ds) {
		return ds.select(col("hotel_id"), datediff(col("srch_co"), col("srch_ci")).as("stay_duration"));
	}
	
}