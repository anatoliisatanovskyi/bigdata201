package anatoliisatanovskyi.bigdata201.pipelining.model;

public class LatLngHttpResponse {

	private java.util.List<Result> results;

	public LatLng extractLatLng() {
		LatLng latLng = null;
		if (results != null && !results.isEmpty()) {
			if (results.get(0).getGeometry() != null) {
				if (results.get(0).getGeometry().getLat() != null && results.get(0).getGeometry().getLng() != null) {
					latLng = new LatLng(results.get(0).getGeometry().getLat(), results.get(0).getGeometry().getLat());
				}
			} else if (results.get(0).getBounds() != null) {
				if (results.get(0).getBounds().getNortheast() != null) {
					if (results.get(0).getBounds().getNortheast().getLat() != null
							&& results.get(0).getBounds().getNortheast().getLng() != null) {
						latLng = new LatLng(results.get(0).getBounds().getNortheast().getLat(),
								results.get(0).getBounds().getNortheast().getLng());
					}
				}
			}
		}
		return latLng;
	}

	public java.util.List<Result> getResults() {
		return results;
	}

	public void setResults(java.util.List<Result> results) {
		this.results = results;
	}

	public static class Result {
		private Bounds bounds;
		private Coords geometry;

		public Bounds getBounds() {
			return bounds;
		}

		public void setBounds(Bounds bounds) {
			this.bounds = bounds;
		}

		public Coords getGeometry() {
			return geometry;
		}

		public void setGeometry(Coords geometry) {
			this.geometry = geometry;
		}
	}

	public static class Bounds {
		private Coords northeast;

		public Coords getNortheast() {
			return northeast;
		}

		public void setNortheast(Coords northeast) {
			this.northeast = northeast;
		}
	}

	public static class Coords {
		private Double lat;
		private Double lng;

		public Double getLat() {
			return lat;
		}

		public void setLat(Double lat) {
			this.lat = lat;
		}

		public Double getLng() {
			return lng;
		}

		public void setLng(Double lng) {
			this.lng = lng;
		}
	}
}
