package anatoliisatanovskyi.bigdata201.pipelining;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import anatoliisatanovskyi.bigdata201.pipelining.model.LatLng;
import anatoliisatanovskyi.bigdata201.pipelining.model.LatLngHttpResponse;

public class Requester {

	private static final String URL_TEMPLATE = "https://api.opencagedata.com/geocode/v1/json?q=%s,%s,%s&key=%s";

	private static final Requester INSTANCE = new Requester();
	
	private final ObjectMapper mapper = new ObjectMapper();
	private Threshold threshold = new Threshold();

	public Requester() {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public static Requester instance() {
		return INSTANCE;
	}

	public LatLng requestLatLng(String country, String city, String address, String apiKey) throws IOException {
		String url = buildURL(country, city, address, apiKey);
		String json = getBodyAsString(url);
		threshold.check();
		return parseAndExtractLatLng(json);
	}

	private String buildURL(String country, String city, String address, String apiKey) {
		return String.format(URL_TEMPLATE, country, city, address, apiKey);
	}

	private String getBodyAsString(String urlStr) throws IOException {
		StringBuilder result = new StringBuilder();
		URL url = new URL(urlStr.replaceAll(" ", "%20"));
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		try (BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return result.toString();
		}
	}

	private LatLng parseAndExtractLatLng(String json) throws JsonMappingException, JsonProcessingException {
		LatLngHttpResponse response = mapper.readValue(json, LatLngHttpResponse.class);
		return response.extractLatLng();
	}
}
