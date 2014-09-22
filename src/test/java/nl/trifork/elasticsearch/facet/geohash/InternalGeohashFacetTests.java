package nl.trifork.elasticsearch.facet.geohash;

import nl.trifork.elasticsearch.clustering.grid.test.Places;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class InternalGeohashFacetTests {

    @Test
    public void testSerializationRoundtrip() throws Exception {

        InternalGeohashFacet facet = new InternalGeohashFacet(
                "name",
                0.5,
                true,
                false,
                Arrays.asList(new Cluster(Places.DENVER,
                        BinaryGeoHashUtils.encodeAsLong(Places.DENVER, BinaryGeoHashUtils.MAX_PREFIX_LENGTH),
                        BinaryGeoHashUtils.MAX_PREFIX_LENGTH, CenteringAlgorithm.ARITHMETIC_MEAN))
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        OutputStreamStreamOutput output = new OutputStreamStreamOutput(byteArrayOutputStream);

        facet.writeTo(output);

        InternalGeohashFacet deserialized = new InternalGeohashFacet();
        deserialized.readFrom(new BytesStreamInput(byteArrayOutputStream.toByteArray(), false));

        assertThat(deserialized.factor(), is(facet.factor()));
        assertThat(deserialized.showGeohashCell(), is(facet.showGeohashCell()));
        assertThat(deserialized.getEntries().size(), is(1));
        assertThat(deserialized.getEntries().get(0).center(), is(Places.DENVER));
        assertThat(deserialized.getEntries().get(0).clusterGeohash(), is(BinaryGeoHashUtils.encodeAsLong(Places.DENVER, BinaryGeoHashUtils.MAX_PREFIX_LENGTH)));
    }
}
