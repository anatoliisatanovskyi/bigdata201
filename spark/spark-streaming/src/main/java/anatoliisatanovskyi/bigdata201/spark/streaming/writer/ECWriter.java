package anatoliisatanovskyi.bigdata201.spark.streaming.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ECWriter implements OutputWriter {

	@Override
	public void write(Dataset<Row> ds, String path) {
		ds.write()//
				.format("org.elasticsearch.spark.sql")//
				.option("es.nodes", "localhost")//
				.option("es.port", "9200")//
				.option("es.index.auto.create", "true")//
				.option("es.resource", path)//
				.mode("append").save();
	}

}
