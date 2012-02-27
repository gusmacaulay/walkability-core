/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.util.Collection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;

import org.geotools.graph.build.feature.FeatureGraphGenerator;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.Node;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

/**
 * Calculates the Connectivity Index - count of 3 legged intersection per square
 * kilometer, from a region and a network.
 *
 * @author gus
 */
public final class ConnectivityIndex {

    /**
     * Private hidden constructor as this is a utility style class
     */
    private ConnectivityIndex() {
    }

    /**
     * Calculates the connectivity of a region upon a network
     *
     * @param featureSource the feature source containing features in the
     * network
     * @param roi the region of interest
     * @return returns the connections per square kilometer in the roi
     * @throws IOException
     */
    public static SimpleFeature connectivity(SimpleFeatureSource featureSource, SimpleFeature roiFeature) throws IOException {

        Geometry roiGeom = (Geometry) roiFeature.getDefaultGeometryProperty().getValue();
        double area = roiGeom.getArea() / 1000000; // converting to sq. km. -- bit dodgy should check units but assuming in metres
        Graph graph = buildLineNetwork(featureSource, roiGeom);
        //Construct a new feature with a "Connectivity" attribute to store connectivity in //
        SimpleFeatureType sft = (SimpleFeatureType) roiFeature.getType();
        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(sft);
        stb.setName("connectivityFeatureType");
        //Add the connectivity attribute
        stb.add("Connectivity", Double.class);
        SimpleFeatureType connectivityFeatureType = stb.buildFeatureType();
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(connectivityFeatureType);
        sfb.addAll(roiFeature.getAttributes());

        double connectivity = countConnections(graph) / area;
        sfb.add(connectivity);
        SimpleFeature connectivityFeature = sfb.buildFeature(null);
        
        return connectivityFeature;
    }

    /**
     * Constructs a geotools Graph line network from a feature source within a
     * given region of interest
     *
     * @param featureSource the network feature source
     * @param roi the region of interest (must be a polygon?)
     * @return returns a geotools Graph based on the features within the region
     * of interest
     * @throws IOException
     */
    private static Graph buildLineNetwork(SimpleFeatureSource featureSource, Geometry roi) throws IOException {
        //Construct a filter which first filters within the bbox of roi and then filters with intersections of roi
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        FeatureType schema = featureSource.getSchema();

        String geometryPropertyName = schema.getGeometryDescriptor().getLocalName();

        Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(roi));

        // get a feature collection of filtered features
        SimpleFeatureCollection fCollection = featureSource.getFeatures(filter);

        //create a linear graph generator
        LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();

        //wrap it in a feature graph generator
        FeatureGraphGenerator featureGen = new FeatureGraphGenerator(lineStringGen);

        //put all the features that intersect the roi into  the graph generator
        FeatureIterator iter = fCollection.features();

        try {
            while (iter.hasNext()) {
                Feature feature = iter.next();
                featureGen.add(feature);
            }
        } finally {
            iter.close();
        }
        return featureGen.getGraph();
    }

    /**
     * Counts all the connections (3 legged nodes) in a graph
     *
     * @param graph the graph to process.
     * @return returns the total number of connections
     */
    private static int countConnections(Graph graph) {
        int count = 0;
        //System.out.println("Nodes: " + graph.getNodes().size() );
        for (Node node : (Collection<Node>) graph.getNodes()) {
            if (node.getEdges().size() >= 3) { //3 or more legged nodes are connected
                //   System.out.println("connection!");
                count++;
            }
        }
        return count;
    }
}
