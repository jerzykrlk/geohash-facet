package nl.trifork.elasticsearch.facet.geohash;

import nl.trifork.elasticsearch.facet.geohash.util.EmbeddedElasticSearch;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:nl/trifork/elasticsearch/facet/geohash/util/elasticSearchHolderContext.xml")
public class GeohashFacetIT {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @SuppressWarnings("UnusedDeclaration")
    @Inject
    private EmbeddedElasticSearch embeddedElasticSearch;

    @Test
    public void testDataLoaded() throws Exception {
        long hits = embeddedElasticSearch.getClient().prepareSearch("theindex").get().getHits().totalHits();
        assertEquals("all data loaded", 10, hits);
    }

    @Test
    public void testFacetSearchWithArithmeticMean() throws Exception {
        GeohashFacet geohashFacet = getFacetFor(54, -1, 50, 2);
        GeoPoint center = geohashFacet.getEntries().get(0).center();
        assertEquals("lat", 51.6d, center.getLat(), 0);
        assertEquals("lon", 0.3d, center.getLon(), 0);
    }

    @Test
    public void testFacetSearchWithMedian() throws Exception {
        SearchRequestBuilder request = getSearchRequest(
                51.519290059804916,
                -0.08982181549072266,
                51.51928871870041,
                -0.08981913328170776,
                CenteringAlgorithm.MEDIAN);
        SearchResponse response = request.get();
        GeohashFacet geohashFacet = response.getFacets().facet("location");

        System.out.println(response);
        GeoPoint center = geohashFacet.getEntries().get(0).center();
        assertEquals("lat", 51.519290000000005d, center.getLat(), 0.000000001d);
        assertEquals("lon", -0.08981999999999998d, center.getLon(), 0);
    }

    private GeohashFacet getFacetFor(int topLeftLat, int topLeftLon, int bottomRightLat, int bottomRightLon) {
        SearchRequestBuilder request = getSearchRequest(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon);
        SearchResponse response = request.get();
        GeohashFacet geohashFacet = response.getFacets().facet("location");
        logger.info(response.toString());
        return geohashFacet;
    }

    private SearchRequestBuilder getSearchRequest(double topLeftLat, double topLeftLon, double bottomRightLat,
                                                  double bottomRightLon) {
        GeoBoundingBoxFilterBuilder boundingBox = getBoundingBox(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon);
        GeoFacetBuilder geoHash = getGeohashFacet();
        FilteredQueryBuilder filteredQuery = getFilteredQuery(boundingBox);
        return getSearchRequest(geoHash, filteredQuery);
    }

    private SearchRequestBuilder getSearchRequest(GeoFacetBuilder geoHash, FilteredQueryBuilder filteredQuery) {
        return embeddedElasticSearch.getClient().prepareSearch(EmbeddedElasticSearch.INDEX).setQuery(filteredQuery)
                .addFacet(geoHash);
    }

    private FilteredQueryBuilder getFilteredQuery(GeoBoundingBoxFilterBuilder boundingBox) {
        return QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boundingBox);
    }

    private GeoFacetBuilder getGeohashFacet() {
        return new GeoFacetBuilder(EmbeddedElasticSearch.TYPE)
                .field("latLon").factor(1).showDocId(true).showGeohashCell(true);
    }

    private GeoBoundingBoxFilterBuilder getBoundingBox(double topLeftLat, double topLeftLon, double bottomRightLat, double bottomRightLon) {
        return FilterBuilders.geoBoundingBoxFilter("latLon")
                .topLeft(topLeftLat, topLeftLon)
                .bottomRight(bottomRightLat, bottomRightLon);
    }

    private SearchRequestBuilder getSearchRequest(double topLeftLat, double topLeftLon, double bottomRightLat, double bottomRightLon, CenteringAlgorithm centeringAlgorithm) {
        GeoBoundingBoxFilterBuilder boundingBox = getBoundingBox(topLeftLat, topLeftLon, bottomRightLat, bottomRightLon);
        GeoFacetBuilder geoHash = getGeohashFacet(centeringAlgorithm);
        FilteredQueryBuilder filteredQuery = getFilteredQuery(boundingBox);
        return getSearchRequest(geoHash, filteredQuery);
    }

    private GeoFacetBuilder getGeohashFacet(CenteringAlgorithm centeringAlgorithm) {
        return new GeoFacetBuilder(EmbeddedElasticSearch.TYPE)
                .field("latLon").factor(1).showDocId(true).showGeohashCell(true).centeringAlgorithm(centeringAlgorithm);
    }

}
