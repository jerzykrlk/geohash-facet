package nl.trifork.elasticsearch.facet.geohash;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.geo.GeoPoint;

import java.util.Map;

/**
 * Modified from the original on https://github.com/zenobase/geocluster-facet/blob/master/src/main/java/com/zenobase/search/facet/geocluster/GeoClusterBuilder.java
 */
public class ClusterBuilder {

	private final int geohashBits;
	private final Map<Long, Cluster> clusters = Maps.newHashMap();
    private CenteringAlgorithm centeringAlgorithm;

    public ClusterBuilder(double factor, CenteringAlgorithm centeringAlgorithm) {
        this.centeringAlgorithm = centeringAlgorithm;
        this.geohashBits = BinaryGeoHashUtils.MAX_PREFIX_LENGTH - (int) Math.round(factor * BinaryGeoHashUtils.MAX_PREFIX_LENGTH);
	}

	public ClusterBuilder add(TypeAndId typeAndId, GeoPoint point) {
        long geohash = BinaryGeoHashUtils.encodeAsLong(point, geohashBits);
        if (clusters.containsKey(geohash)) {
            clusters.get(geohash).add(point);
        }
        else {
            if (typeAndId == null) {

                clusters.put(geohash, new Cluster(point, geohash, geohashBits, centeringAlgorithm));
            } else {

                clusters.put(geohash, new Cluster(point, geohash, geohashBits, typeAndId, centeringAlgorithm));
            }
        }
		return this;
    }

	public ClusterBuilder add(GeoPoint point) {
        return add(null, point);
	}

	public ImmutableList<Cluster> build() {
		return ImmutableList.copyOf(clusters.values());
	}

}
