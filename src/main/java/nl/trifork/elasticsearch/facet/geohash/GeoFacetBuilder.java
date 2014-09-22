package nl.trifork.elasticsearch.facet.geohash;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;
import org.elasticsearch.search.facet.FacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacet;

import java.io.IOException;
import java.util.Map;

public class GeoFacetBuilder extends FacetBuilder {
    private String fieldName;
    private double factor;
    private boolean showGeohashCell;
    private boolean showDocId;
    private CenteringAlgorithm centeringAlgorithm = CenteringAlgorithm.ARITHMETIC_MEAN;

    /**
     * Construct a new term facet with the provided facet name.
     *
     * @param name The facet name.
     */
    public GeoFacetBuilder(String name) {
        super(name);
    }

    /**
     * The field the terms will be collected from.
     */
    public GeoFacetBuilder field(String field) {
        this.fieldName = field;
        return this;
    }

    public GeoFacetBuilder showGeohashCell(boolean showGeohashCell) {
        this.showGeohashCell = showGeohashCell;
        return this;
    }

    public GeoFacetBuilder showDocId(boolean showDocId) {
        this.showDocId = showDocId;
        return this;
    }

    public GeoFacetBuilder factor(double factor) {
        this.factor = factor;
        return this;
    }

    public GeoFacetBuilder centeringAlgorithm(CenteringAlgorithm centeringAlgorithm) {
        this.centeringAlgorithm = centeringAlgorithm;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fieldName == null) {
            throw new SearchSourceBuilderException("field must be set facet for facet [" + name + "]");
        }
        builder.startObject(name);

        builder.startObject("geohash");
        builder.field("field", fieldName);
        builder.field("factor", factor);
        builder.field("show_geohash_cell", showGeohashCell);
        builder.field("show_doc_id", showDocId);
        builder.field("centering_algorithm", centeringAlgorithm);

        builder.endObject();
        addFilterFacetAndGlobal(builder, params);
        builder.endObject();
        return builder;
    }
}
