package nl.trifork.elasticsearch.facet.geohash;

import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

/**
 * Modified from the original on https://github.com/zenobase/geocluster-facet/blob/master/src/main/java/com/zenobase/search/facet/geocluster/GeoCluster.java
 */
public class Cluster {

    private int geohashBits;

    private int size;
    private GeoPoint center;
    private long clusterGeohash;
    private TypeAndId typeAndId;
    private BoundingBox bounds;
    private final CenteringAlgorithm centeringAlgorithm;
    private List<GeoPoint> geoPoints = new LinkedList<GeoPoint>();


    /**
     * @param clusterGeohash - geohash of the cluster, obtained from {@link nl.trifork.elasticsearch.facet.geohash.BinaryGeoHashUtils#encodeAsLong(org.elasticsearch.common.geo.GeoPoint, int)}
     * @param geohashBits    - number of meaningful bits of the geohash. Values: 0 to {@link nl.trifork.elasticsearch.facet.geohash.BinaryGeoHashUtils#MAX_PREFIX_LENGTH}
     */
    public Cluster(GeoPoint point, long clusterGeohash, int geohashBits, CenteringAlgorithm centeringAlgorithm) {
        this(1, point, new LinkedList<GeoPoint>(), clusterGeohash, geohashBits, new BoundingBox(point), centeringAlgorithm);
    }

    public Cluster(GeoPoint point, long clusterGeohash, int geohashBits, TypeAndId typeAndId, CenteringAlgorithm centeringAlgorithm) {
        this(1, point, new LinkedList<GeoPoint>(), clusterGeohash, geohashBits, typeAndId, new BoundingBox(point), centeringAlgorithm);
    }

    /**
     * @param clusterGeohash - geohash of the cluster, obtained from {@link nl.trifork.elasticsearch.facet.geohash.BinaryGeoHashUtils#encodeAsLong(org.elasticsearch.common.geo.GeoPoint, int)}
     * @param geohashBits    - number of meaningful bits of the geohash. Values: 0 to {@link nl.trifork.elasticsearch.facet.geohash.BinaryGeoHashUtils#MAX_PREFIX_LENGTH}
     */
    public Cluster(int size, GeoPoint center, List<GeoPoint> geoPoints, long clusterGeohash, int geohashBits, TypeAndId typeAndId, BoundingBox bounds, CenteringAlgorithm centeringAlgorithm) {
        Preconditions.checkArgument(clusterGeohash == BinaryGeoHashUtils.encodeAsLong(center, geohashBits));

        this.size = size;
        this.center = center;
        this.clusterGeohash = clusterGeohash;
        this.geohashBits = geohashBits;
        this.typeAndId = typeAndId;
        this.bounds = bounds;
        this.centeringAlgorithm = centeringAlgorithm;
        if (centeringAlgorithm == CenteringAlgorithm.MEDIAN) {
            geoPoints.addAll(geoPoints);
        }
    }

    /**
     * @param clusterGeohash - geohash of the cluster, obtained from {@link nl.trifork.elasticsearch.facet.geohash.BinaryGeoHashUtils#encodeAsLong(org.elasticsearch.common.geo.GeoPoint, int)}
     * @param geohashBits    - number of meaningful bits of the geohash. Values: 0 to {@link nl.trifork.elasticsearch.facet.geohash.BinaryGeoHashUtils#MAX_PREFIX_LENGTH}
     */
    public Cluster(int size, GeoPoint center, List<GeoPoint> geoPoints, long clusterGeohash, int geohashBits, BoundingBox bounds, CenteringAlgorithm centeringAlgorithm) {

        this(size, center, geoPoints, clusterGeohash, geohashBits, null, bounds, centeringAlgorithm);
    }

    public void add(GeoPoint point) {
        Preconditions.checkArgument(clusterGeohash == BinaryGeoHashUtils.encodeAsLong(point, geohashBits));

        ++size;
        center = mean(center, size - 1, point, 1);
        bounds = bounds.extend(point);
        if (centeringAlgorithm == CenteringAlgorithm.MEDIAN) {
            geoPoints.add(point);
        }
    }

    public Cluster merge(Cluster that) {
        Preconditions.checkArgument(clusterGeohash == that.clusterGeohash &&
                geohashBits == that.geohashBits);

//        GeoPoint center = mean(this.center, this.size(), that.center(), that.size());
        if (centeringAlgorithm == CenteringAlgorithm.MEDIAN) {
            geoPoints.addAll(that.geoPoints);
        }
        return new Cluster(this.size + that.size(),
                center(), geoPoints, this.clusterGeohash, this.geohashBits, this.bounds.extend(that.bounds()), centeringAlgorithm);
    }

    private static GeoPoint mean(GeoPoint left, int leftWeight, GeoPoint right, int rightWeight) {
        double lat = (left.getLat() * leftWeight + right.getLat() * rightWeight) / (leftWeight + rightWeight);
        double lon = (left.getLon() * leftWeight + right.getLon() * rightWeight) / (leftWeight + rightWeight);
        return new GeoPoint(lat, lon);
    }

    public int size() {
        return size;
    }

    public GeoPoint center() {
        if (geoPoints.size() == 0) {
            return center;
        } else {
            double lat = getLat(geoPoints);
            double lon = getLon(geoPoints);
            return new GeoPoint(lat, lon);
//            throw new RuntimeException("new implementation");
        }
    }

    private double getLat(List<GeoPoint> geoPoints) {
        List<Double> lats = new ArrayList<Double>();
        for (GeoPoint geoPoint : geoPoints) {
            lats.add(geoPoint.getLat());
        }
        Collections.sort(lats);
        return lats.get(lats.size() / 2);
    }

    private double getLon(List<GeoPoint> geoPoints) {
        List<Double> lons = new ArrayList<Double>();
        for (GeoPoint geoPoint : geoPoints) {
            lons.add(geoPoint.getLon());
        }
        Collections.sort(lons);
        return lons.get(lons.size() / 2);
    }

    public BoundingBox bounds() {
        return bounds;
    }

    public long clusterGeohash() {
        return clusterGeohash;
    }

    public int clusterGeohashBits() {
        return geohashBits;
    }

    public TypeAndId typeAndId() {
        return typeAndId;
    }

    public static Cluster readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        GeoPoint center = GeoPoints.readFrom(in);
        long clusterGeohash = in.readLong();
        int geohashBits = in.readVInt();
        CenteringAlgorithm centeringAlgorithm = CenteringAlgorithm.valueOf(in.readString());
        LinkedList<GeoPoint> geoPoints = new LinkedList<GeoPoint>();
        if (size > 1) {

            BoundingBox bounds = BoundingBox.readFrom(in);
            return new Cluster(size, center, geoPoints, clusterGeohash, geohashBits, bounds, centeringAlgorithm);
        } else {

            BoundingBox bounds = new BoundingBox(center, center);
            boolean hasDocId = in.readBoolean();
            if (hasDocId) {

                TypeAndId typeAndId1 = TypeAndId.readFrom(in);
                return new Cluster(size, center, geoPoints, clusterGeohash, geohashBits, typeAndId1, bounds, centeringAlgorithm);
            } else {

                return new Cluster(size, center, geoPoints, clusterGeohash, geohashBits, bounds, centeringAlgorithm);
            }
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        if (centeringAlgorithm==CenteringAlgorithm.MEDIAN) {
            GeoPoints.writeTo(center(), out);
        }else{
            GeoPoints.writeTo(center, out);
        }
        out.writeLong(clusterGeohash);
        out.writeVInt(geohashBits);
        out.writeString(centeringAlgorithm.name());
        if (size > 1) {
            bounds.writeTo(out);
        } else {
            if (typeAndId == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                typeAndId.writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cluster cluster = (Cluster) o;

        if (clusterGeohash != cluster.clusterGeohash) return false;
        if (geohashBits != cluster.geohashBits) return false;
        if (size != cluster.size) return false;
        if (!bounds.equals(cluster.bounds)) return false;
        if (!center.equals(cluster.center)) return false;
        if (!centeringAlgorithm.equals(cluster.centeringAlgorithm)) return false;
        if (typeAndId != null ? !typeAndId.equals(cluster.typeAndId) : cluster.typeAndId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = geohashBits;
        result = 31 * result + size;
        result = 31 * result + center.hashCode();
        result = 31 * result + (int) (clusterGeohash ^ (clusterGeohash >>> 32));
        result = 31 * result + (typeAndId != null ? typeAndId.hashCode() : 0);
        result = 31 * result + bounds.hashCode();
        result = 31 * result + centeringAlgorithm.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s %s (%d)", GeoPoints.toString(center), clusterGeohash, size);
    }
}
