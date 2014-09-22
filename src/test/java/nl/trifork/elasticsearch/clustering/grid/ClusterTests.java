package nl.trifork.elasticsearch.clustering.grid;

import static nl.trifork.elasticsearch.clustering.grid.test.GeoPointMatchers.closeTo;
import static nl.trifork.elasticsearch.clustering.grid.test.Places.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.Random;

import nl.trifork.elasticsearch.facet.geohash.*;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.testng.annotations.Test;

public class ClusterTests {

    private Random random = new Random(System.currentTimeMillis());

    @Test
    public void serializationRoundtrip() throws IOException {

        for (int i = 0; i < 100; i++) {

            GeoPoint point = new GeoPoint(random.nextDouble() * 180 - 90, random.nextDouble() * 360 - 180);

            for (int geohashBits = 0; geohashBits <= BinaryGeoHashUtils.MAX_PREFIX_LENGTH; geohashBits++) {

                System.out.printf("Testing with point %s and %d geohash bit(s)...\n", point.toString(), geohashBits);

                Cluster cluster = new Cluster(point, BinaryGeoHashUtils.encodeAsLong(point, geohashBits), geohashBits, CenteringAlgorithm.ARITHMETIC_MEAN);
                assertThat(cluster, equalTo(roundtrip(cluster)));
            }
        }

    }

    private Cluster roundtrip(Cluster cluster) throws IOException {

        BytesStreamOutput out = new BytesStreamOutput();
        cluster.writeTo(out);

        return Cluster.readFrom(new BytesStreamInput(out.bytes()));
    }


}
