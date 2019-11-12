package anatoliisatanovskyi.bigdata201.pipelining;

import java.io.IOException;

import anatoliisatanovskyi.bigdata201.pipelining.exception.CoordsProcessorException;
import anatoliisatanovskyi.bigdata201.pipelining.model.LatLng;

public class CoordsProcessor {

	private final String apiKey;
	private final int geohashLength;

	public CoordsProcessor(String apiKey, int geohashLength) {
		this.apiKey = apiKey;
		this.geohashLength = geohashLength;
	}

	/**
	 * @param lat - latitude
	 * @param lng - longitude
	 * @return geohash for latitude & longitude coordinates. In case of invalid
	 *         values - returns null
	 */
	public String processToGeohash(String lat, String lng) {
		return processToGeohash(new LatLng(lat, lng));
	}

	private String processToGeohash(LatLng latLng) {
		return latLng != null && latLng.validate() ? latLng.geohash(geohashLength) : null;
	}

	/**
	 * Makes 3rd party api call to fetch coordinates for given country, city and
	 * address.
	 * 
	 * @return geohash for latitude & longitude coordinates. In case of invalid
	 *         values - returns null
	 * @throws CoordsProcessorException in case of 3rd party api call error
	 */
	public String processToGeohash(String country, String city, String address) throws CoordsProcessorException {
		try {
			LatLng latLng = Requester.instance().requestLatLng(country, city, address, apiKey);
			return processToGeohash(latLng);
		} catch (IOException e) {
			throw new CoordsProcessorException("An error occurred while retrieving coordinates:" + e.getMessage(), e);
		}
	}
}