/*
 * Copyright (C) 2012 amacaulay
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.mccaughey.connectivity;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;

import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.graph.build.feature.FeatureGraphGenerator;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.path.Path;
import org.geotools.graph.path.Walk;
import org.geotools.graph.structure.Edge;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.Node;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public final class NetworkBuffer {

    static final Logger LOGGER = LoggerFactory.getLogger(NetworkBuffer.class);

    private NetworkBuffer() {
    }

    public static SimpleFeature createPolygonBuffer(SimpleFeatureSource network, SimpleFeature pointFeature, Double distance) throws IOException {


        //Build network Graph - within bounds
        Point pointOfInterest = (Point) pointFeature.getDefaultGeometry();

        Geometry pointBuffer = pointOfInterest.buffer(distance);

        SimpleFeatureCollection networkRegion = featuresInRegion(network, pointBuffer);
        FeatureGraphGenerator networkGraphGen = buildFeatureNetwork(networkRegion);
        // Graph networkGraph = networkGraphGen.getGraph();
        //Snap point to network
        final SpatialIndex index = new STRtree();

        // Create line string index
        // Just in case: check for  null or empty geometry
        SimpleFeatureIterator features = network.getFeatures().features();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            Geometry geom = (Geometry) (feature.getDefaultGeometry());
            if (geom != null) {
                Envelope env = geom.getEnvelopeInternal();
                if (!env.isNull()) {
                    index.insert(env, new LocationIndexedLine(geom));
                }
            }
        }

        Coordinate pt = pointOfInterest.getCoordinate();
        Envelope search = new Envelope(pt);
        search.expandBy(distance);

        /*
         * Query the spatial index for objects within the search envelope. Note
         * that this just compares the point envelope to the line envelopes so
         * it is possible that the point is actually more distant than
         * MAX_SEARCH_DISTANCE from a line.
         */
        List<LocationIndexedLine> lines = index.query(search);

        // Initialize the minimum distance found to our maximum acceptable
        // distance plus a little bit
        double minDist = distance + 1.0e-6;
        Coordinate minDistPoint = null;

        for (LocationIndexedLine line : lines) {

            LinearLocation here = line.project(pt); //What does project do?
            Coordinate point = line.extractPoint(here); //What does extracPoint do?
            double dist = point.distance(pt);
            if (dist < minDist) {
                minDist = dist;
                minDistPoint = point;
            }
        }


        if (minDistPoint == null) {
            // No line close enough to snap the point to
            LOGGER.info(pt + "- X");

        } else {
            LOGGER.info("{} - snapped by moving {}\n", pt.toString(), minDist);
            pointFeature.setDefaultGeometry(pointOfInterest);
            // System.out.println("Old network features: " + String.valueOf(networkRegion.size()));
            GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);

            Coordinate[] coords =
                    new Coordinate[]{new Coordinate(pt.x, pt.y), new Coordinate(minDistPoint.x, minDistPoint.y)};

            LineString snapLine = geometryFactory.createLineString(coords);
            networkGraphGen.add(buildFeatureFromGeometry(networkRegion.features().next(), snapLine));
            //feature.setDefaultGeometry(pointOfInterest);
//            System.out.println("Added snapped node");
//            //Graph networkGraph2 = networkGraphGen.getGraph();
//            System.out.println("Orginal nodes: " + String.valueOf(networkRegion.size()));
//            System.out.println("New nodes: " + String.valueOf(networkGraph.getNodes().size()));
            Graph graph = networkGraphGen.getGraph();
            Path startPath = new Path();
            Node startNode = (Node) graph.getNodes().toArray()[0]; //hmm is the snap node the last node?
            startPath.add(startNode);
            List<Path> paths = findPaths(graph, startPath);
            LOGGER.info("Found paths: " + String.valueOf(paths.size()));
            LOGGER.info(pathsToJSON(paths));
            //  LOGGER.info(graphToJSON(graph));
        }


        //

        //Follow paths out from point/node of interest, cut lines at distance
        //        List<Graph> paths = findPaths(startNode,null);
        //create buffer of subnetwork, polygonize


        return pointFeature;
    }

    private static String graphToJSON(Graph graph) {
        String json = "";
        List<SimpleFeature> features = new ArrayList();

        for (Edge edge : (Collection<Edge>) graph.getEdges()) {
            features.add(((SimpleFeature) edge.getObject()));
        }
        return (writeFeatures(DataUtilities.collection(features)));
    }

    private static String walksToJSON(List<Walk> walks) {
        String json = "";
        List<SimpleFeature> features = new ArrayList();
        for (Walk walk : walks) {
            LOGGER.info("Walk: " + String.valueOf(walk.getEdges()));
            for (Edge edge : (List<Edge>) walk.getEdges()) {
                features.add(((SimpleFeature) edge.getObject()));
            }
            //  LOGGER.info(writeFeatures(DataUtilities.collection(features)));
        }

        return (writeFeatures(DataUtilities.collection(features)));
    }

    private static String pathsToJSON(List<Path> paths) {
        String json = "";
        List<SimpleFeature> features = new ArrayList();
        for (Path path : paths) {
            LOGGER.info("Path: " + String.valueOf(path.getEdges()));
            for (Edge edge : (List<Edge>) path.getEdges()) {
                features.add(((SimpleFeature) edge.getObject()));
            }
            //  LOGGER.info(writeFeatures(DataUtilities.collection(features)));
        }

        return (writeFeatures(DataUtilities.collection(features)));
    }

    private static String writeFeatures(SimpleFeatureCollection features) {
        FeatureJSON fjson = new FeatureJSON();
        Writer writer = new StringWriter();
        try {
            fjson.writeFeatureCollection(features, writer);
        } catch (Exception e) {
            return "{}";
        }
        return writer.toString();

    }

     private static List<Path> findPaths(Graph network, Path currentPath) {
        List<Path> paths = new ArrayList();

        //for each edge connected to current node 
        for (Node node : (Collection<Node>) network.getNodes()) { //find the current node in the graph (not very efficient)
            if (node.equals(currentPath.getLast())) {
//                LOGGER.info("Found currentNode in network graph: " + currentPath.getLast());
                for (Edge graphEdge : (List<Edge>) node.getEdges()) {
//                    LOGGER.info("Graph Node Edges: " + node.getEdges());
//                    LOGGER.info("Path Node Edges: " + currentPath.getEdges());
//                    LOGGER.info("Found new connected edge: " + graphEdge);
                    if (currentPath.size() < 8) { //if path + edge less than distance

                        //append findPaths(path+edge) to list of paths
                        //Path nextpath = (Path)currentpath.clone();
                        //   Path nextpath = addEdge(graphEdge, currentpath);
                        Path nextpath = new Path();
                        nextpath.addEdges(currentPath.getEdges());
                        if (nextpath.addEdge(graphEdge)) {
//                            LOGGER.info("Exploring path beyond new edge: " + nextpath.getEdges());
                            if (nextpath.isValid())
                                paths.addAll(findPaths(network, nextpath));
                        }
                    } else {//else chop edge, append (path + chopped edge) to list of paths
//                        LOGGER.info("Adding new (chopped) edge: " + graphEdge + " to current Path: " + currentPath.getEdges());
                        //  Path newpath = addEdge(graphEdge, currentpath);
                        Path newpath = new Path();
                        newpath.addEdges(currentPath.getEdges());
                        if (newpath.addEdge(graphEdge)) {
//                            LOGGER.info("New Path: " + newpath.getEdges());
                            if (newpath.isValid())
                                paths.add(newpath);
                        }
                    }
                }
                //return all paths
                return paths;
            }
        }
        LOGGER.info("FAIL!");
        return null;
    }
    
    private static List<Walk> findWalks(Graph network, Walk currentWalk) {
        List<Walk> walks = new ArrayList();

        //for each edge connected to current node 
        for (Node node : (Collection<Node>) network.getNodes()) { //find the current node in the graph (not very efficient)
            if (node.equals(currentWalk.getLast())) {
                LOGGER.info("Found currentNode in network graph: " + currentWalk.getLast());
                for (Edge graphEdge : (List<Edge>) node.getEdges()) {
                    LOGGER.info("Graph Node Edges: " + node.getEdges());
                    LOGGER.info("Path Node Edges: " + currentWalk.getEdges());
                    LOGGER.info("Found new connected edge: " + graphEdge);
                    if (currentWalk.size() < 8) { //if path + edge less than distance

                        //append findPaths(path+edge) to list of paths
                        //Walk nextWalk = (Walk)currentWalk.clone();
                        //   Walk nextWalk = addEdge(graphEdge, currentWalk);
                        Walk nextWalk = new Walk();
                        nextWalk.addEdges(currentWalk.getEdges());
                        if (nextWalk.addEdge(graphEdge)) {
                            LOGGER.info("Exploring path beyond new edge: " + nextWalk.getEdges());
                            walks.addAll(findWalks(network, nextWalk));
                        }
                    } else {//else chop edge, append (path + chopped edge) to list of paths
                        LOGGER.info("Adding new (chopped) edge: " + graphEdge + " to current walk: " + currentWalk.getEdges());
                        //  Walk newWalk = addEdge(graphEdge, currentWalk);
                        Walk newWalk = new Walk();
                        newWalk.addEdges(currentWalk.getEdges());
                        if (newWalk.addEdge(graphEdge)) {
                            LOGGER.info("New Walk: " + newWalk.getEdges());
                            walks.add(newWalk);
                        }
                    }
                }

                //return all paths
                return walks;
            }
        }
        LOGGER.info("FAIL!");
        return null;
    }

    static public Walk addEdge(Edge e, Walk walk) {
        //append edge to end of path, path must be empty, or last node in path
        // must be a node of the edge

//  	//save current edge list
//  	List edges = walk.getEdges();
        Walk newWalk = new Walk();
        newWalk.addEdges(walk.getEdges());

        if (newWalk.isEmpty()) {
            //add both nodes
            newWalk.add(e.getNodeA());
            newWalk.add(e.getNodeB());
        } else {
            //walk is not empty, check to see if the last node is related to the edge
            Node last = newWalk.getLast();
            LOGGER.info("Node Last: " + last + " Node A: " + e.getNodeA() + " Node B: " + e.getNodeB());
            if (last.equals(e.getNodeA())) {
                newWalk.add(e.getNodeB());
            } else if (last.equals(e.getNodeB())) {
                newWalk.add(e.getNodeA());
            } else {
                LOGGER.info("WTF??");
                return null;
            }
        }

        //the addition of nodes resets the internal edge list so it must be rebuilt.
        // In the case that an edge shares both of its nodes with another edge
        // it is possible for the list to be rebuilt properly (ie. not contain
        // the edge being added). To rectify this situation, a backup copy of the 
        // edge list is saved before the addition, the addition performed, the 
        // edge explicitly added to the backup edge list, and the internal
        // edge list replaced by the modified backup
//	 edges.add(e);


        //walk.m_edges = edges;

        return newWalk;
    }

    private static SimpleFeature buildFeatureFromGeometry(SimpleFeature feature, Geometry geom) {

        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(feature.getFeatureType());
        //stb.setName("connectivityFeatureType");
        //Add the connectivity attribute
//        stb.add("Connectivity", Double.class);
//        SimpleFeatureType connectivityFeatureType = stb.buildFeatureType();
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(feature.getFeatureType());
        //  sfb.addAll(feature.getAttributes());
        sfb.add(geom);

        return sfb.buildFeature(null);
    }

    private static SimpleFeatureCollection featuresInRegion(SimpleFeatureSource featureSource, Geometry roi) throws IOException {
        //Construct a filter which first filters within the bbox of roi and then filters with intersections of roi
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        FeatureType schema = featureSource.getSchema();

        String geometryPropertyName = schema.getGeometryDescriptor().getLocalName();

        Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(roi));

        // collection of filtered features
        return featureSource.getFeatures(filter);
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
    private static FeatureGraphGenerator buildFeatureNetwork(SimpleFeatureCollection featureCollection) {
        //create a linear graph generator
        LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();

        //wrap it in a feature graph generator
        FeatureGraphGenerator featureGen = new FeatureGraphGenerator(lineStringGen);

        //put all the features into  the graph generator
        FeatureIterator iter = featureCollection.features();

        try {
            while (iter.hasNext()) {
                Feature feature = iter.next();
                featureGen.add(feature);
            }
        } finally {
            iter.close();
        }
        return featureGen;
    }
}
