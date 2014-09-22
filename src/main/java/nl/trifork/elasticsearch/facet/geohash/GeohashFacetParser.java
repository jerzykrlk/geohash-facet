package nl.trifork.elasticsearch.facet.geohash;

import java.io.IOException;

import org.elasticsearch.common.base.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetExecutor.Mode;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Modified from the original on https://github.com/zenobase/geocluster-facet/blob/master/src/main/java/com/zenobase/search/facet/geocluster/GeoClusterFacetParser.java
 */
public class GeohashFacetParser extends AbstractComponent implements FacetParser {

    @Inject
	public GeohashFacetParser(Settings settings) {
		super(settings);
		InternalGeohashFacet.registerStreams();
	}

	@Override
	public String[] types() {
		return new String[] {
			GeohashFacet.TYPE
		};
	}

	@Override
	public Mode defaultMainMode() {
		return FacetExecutor.Mode.COLLECTOR;
	}

	@Override
	public Mode defaultGlobalMode() {
		return FacetExecutor.Mode.COLLECTOR;
	}

	@Override
	public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {

		String fieldName = null;
		double factor = 0.1;
        boolean showGeohashCell = false;
        boolean showDocumentId = false;
        String centeringAlgorithmName = null;

        String currentName = parser.currentName();
		XContentParser.Token token;
		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
			if (token == XContentParser.Token.FIELD_NAME) {
				currentName = parser.currentName();
			} else if (token.isValue()) {
				if ("field".equals(currentName)) {
					fieldName = parser.text();
				} else if ("factor".equals(currentName)) {
					factor = parser.doubleValue();
				} else if ("show_geohash_cell".equals(currentName)) {
					showGeohashCell = parser.booleanValue();
				} else if ("show_doc_id".equals(currentName)) {
                    showDocumentId = parser.booleanValue();
                }else if ("centering_algorithm".equals(currentName)) {
                    centeringAlgorithmName = parser.text();
                }
			}
		}

		if (factor < 0.0 || factor > 1.0) {
			throw new FacetPhaseExecutionException(facetName, "value [" + factor + "] is not in range [0.0, 1.0]");
		}
		FieldMapper<?> fieldMapper = context.smartNameFieldMapper(fieldName);
        if (fieldMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "failed to find mapping for [" + fieldName + "]");
        }
        IndexGeoPointFieldData<?> indexFieldData = context.fieldData().getForField(fieldMapper);

        FieldMapper<?> idFieldMapper = context.smartNameFieldMapper("_uid");
        if (idFieldMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "failed to find mapping for [_id]");
        }
        IndexFieldData<?> idIndexFieldData = context.fieldData().getForField(idFieldMapper);

        CenteringAlgorithm centeringAlgorithm = getCenteringAlgorithm(facetName, centeringAlgorithmName);

		return new GeohashFacetExecutor(indexFieldData, idIndexFieldData, factor, showGeohashCell, showDocumentId, centeringAlgorithm);
	}

    private CenteringAlgorithm getCenteringAlgorithm(String facetName, String centeringAlgorithmName) {
        if (Strings.isNullOrEmpty(centeringAlgorithmName)){
            return CenteringAlgorithm.ARITHMETIC_MEAN;
        }
        try{
            return CenteringAlgorithm.valueOf(centeringAlgorithmName);
        }catch (IllegalArgumentException e){
            throw new FacetPhaseExecutionException(facetName, "failed to find centering algorithm ["+centeringAlgorithmName+"]");
        }
    }
}
