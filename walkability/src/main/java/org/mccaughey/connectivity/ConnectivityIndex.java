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

import org.geotools.graph.build.feature.FeatureGraphGenerator;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.Node;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

/**
 * Calculates the Connectivity Index - count of 3 legged intersection per square
 * kilometer, from a region and a network.
 *
 * @author gus
 */
public class ConnectivityIndex {

    /**
     * Calculates the connectivity of a region upon a network
     *
     * @param featureSource the feature source containing features in the
     * network
     * @param roi the region of interest
     * @return returns the connections per square kilometer in the roi
     * @throws IOException
     */
    public static double connectivity(SimpleFeatureSource featureSource, Geometry roi) throws IOException {

        double area = roi.getArea() / 1000000; // converting to sq. km. -- bit dodgy should check units but assuming in metres
        Graph graph = buildLineNetwork(featureSource, roi);
        //System.out.println("Area:" + String.valueOf(area) + " Connections:" + String.valueOf(countConnections(graph)));
        return countConnections(graph) / area;
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

        Filter filter = ff.intersects(ff.property(geometryPropertyName),ff.literal(roi));

        // get a feature collection of filtered features
        SimpleFeatureCollection fCollection = featureSource.getFeatures(filter);

        //create a linear graph generate
        LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();

        //wrap it in a feature graph generator
        FeatureGraphGenerator featureGen = new FeatureGraphGenerator(lineStringGen);

        //put all the features that intersect the roi into  the graph generator
        FeatureIterator iter = fCollection.features();

        try {
            while (iter.hasNext()) {
                Feature feature = iter.next();
               // if (roi.intersects((Geometry) feature.getDefaultGeometryProperty().getValue())) {
                featureGen.add(feature);
                    // System.out.println("interesected!");
               // }
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
