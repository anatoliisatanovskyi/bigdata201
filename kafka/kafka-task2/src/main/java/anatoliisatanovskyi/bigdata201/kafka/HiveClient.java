package anatoliisatanovskyi.bigdata201.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import anatoliisatanovskyi.bigdata201.kafka.model.Hotel;

public class HiveClient implements Closeable {

	private Config config = Config.instance();

	private Connection connection;

	public HiveClient() {
	}

	public void connect() throws SQLException {
		this.connection = createConnection();
	}

	public List<Hotel> extract() throws SQLException {

		Statement statement = connection.createStatement();
		String sql = String.format("SELECT * FROM %s", config.hive().getInputTable());
		ResultSet resultSet = statement.executeQuery(sql);
		return parseQueryResult(resultSet);
	}

	private Connection createConnection() throws SQLException {
		String connectUrl = String.format("jdbc:hive2://%s:%d/default", config.hive().getHostname(),
				config.hive().getPort());
		return DriverManager.getConnection(connectUrl, "hive", "");
	}

	private List<Hotel> parseQueryResult(ResultSet rs) throws SQLException {
		List<Hotel> list = new ArrayList<>();
		while (rs.next()) {
			String name = rs.getNString("name");
			String country = rs.getNString("country");
			String city = rs.getNString("city");
			String address = rs.getNString("address");
			Double latitude = rs.getDouble("latitude");
			Double longitude = rs.getDouble("longitude");
			String geohash = rs.getNString("geohash");
			Hotel hotel = new Hotel(name, country, city, address, String.valueOf(latitude), String.valueOf(longitude),
					geohash);
			list.add(hotel);
		}
		return list;
	}

	@Override
	public void close() throws IOException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new IOException("error closing db connection: " + e.getMessage(), e);
			}
		}
	}
}