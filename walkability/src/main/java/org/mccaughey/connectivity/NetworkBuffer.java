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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geometry.jts.GeometryCollector;
import org.geotools.graph.build.feature.FeatureGraphGenerator;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.path.Path;
import org.geotools.graph.structure.Edge;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.Node;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates Network Buffers, which can be used as service areas
 *
 * @author amacaulay
 */
public final class NetworkBuffer {

    static final Logger LOGGER = LoggerFactory.getLogger(NetworkBuffer.class);
    private static PrecisionModel precision = new PrecisionModel(10);  //FIXME: should be configurable

    private NetworkBuffer() {
    }

    /**
     *
     * @param network A network (eg roads) dataset
     * @param pointFeature A point of interest used as a starting point
     * @param networkDistance The distance to traverse along the network
     * @param bufferDistance The distance to buffer the network to create the final region
     * @return A network of all paths of networkDistance from the starting point (snapped to the network)
     * @throws IOException
     */
    public static Map findServiceArea(SimpleFeatureSource network, SimpleFeature pointFeature, Double networkDistance, Double bufferDistance) throws IOException {

        Point pointOfInterest = (Point) pointFeature.getDefaultGeometry();
        LocationIndexedLine nearestLine = findNearestEdgeLine(network, networkDistance, bufferDistance, pointOfInterest);
        if (nearestLine == null) {
            return null;
        }
        Geometry pointBuffer = pointOfInterest.buffer(networkDistance + bufferDistance);
        SimpleFeatureCollection networkRegion = featuresInRegion(network, pointBuffer);
        Path startPath = new Path();
        Graph networkGraph = createGraphWithStartNode(nearestLine, startPath, networkRegion, pointOfInterest);
        Map networkMap = graphToMap(networkGraph);
        Map serviceArea = new ConcurrentHashMap();
        NetworkBufferFJ nbfj = new NetworkBufferFJ(networkMap, startPath, networkDistance, serviceArea);
        serviceArea = nbfj.createBuffer();
        LOGGER.info("Found " + serviceArea.size() + " Edges");
        writeNetworkFromEdges(serviceArea);
        return serviceArea;
    }

    private static Graph createGraphWithStartNode(LocationIndexedLine connectedLine, Path startPath, SimpleFeatureCollection networkRegion, Point pointOfInterest) {
        Coordinate pt = pointOfInterest.getCoordinate();
        LinearLocation here = connectedLine.project(pt);
        Coordinate minDistPoint = connectedLine.extractPoint(here);
        Geometry lineA = connectedLine.extractLine(connectedLine.getStartIndex(), connectedLine.project(minDistPoint));
        Geometry lineB = connectedLine.extractLine(connectedLine.project(minDistPoint), connectedLine.getEndIndex());
        Geometry originalLine = connectedLine.extractLine(connectedLine.getStartIndex(), connectedLine.getEndIndex());

        if ((lineB.getLength() == 0.0) || (lineA.getLength() == 0.0)) {

            //   LOGGER.info("Line length: " + originalLine.getLength());
            FeatureGraphGenerator networkGraphGen = buildFeatureNetwork(networkRegion);
            //SimpleFeature featureA = buildFeatureFromGeometry(networkRegion.getSchema(), lineA);
            //networkGraphGen.add(featureA);
            Graph graph = networkGraphGen.getGraph();
            startPath.add(findStartNode(graph, originalLine));
            return graph;
        } else {
            removeFeature(networkRegion, originalLine);



            GeometryFactory gf = new GeometryFactory(precision);
            lineA = gf.createLineString(lineA.getCoordinates());
            lineB = gf.createLineString(lineB.getCoordinates());
            LineString[] lines = new LineString[]{(LineString) lineB};

            SimpleFeatureType edgeType = createEdgeFeatureType(networkRegion.getSchema().getCoordinateReferenceSystem());
            SimpleFeature featureB = buildFeatureFromGeometry(edgeType, lineB);

            SimpleFeature featureA = buildFeatureFromGeometry(edgeType, lineA);

            FeatureGraphGenerator networkGraphGen = buildFeatureNetwork(networkRegion);
            networkGraphGen.add(featureA);
            networkGraphGen.add(featureB);

            Graph graph = networkGraphGen.getGraph();
            startPath.add(findStartNode(graph, featureA, featureB));

            return graph;

        }
    }

    /**
     * Constructs a geotools Graph line network from a feature source
     *
     * @param featureCollection the network feature collection
     * @return returns a geotools FeatureGraphGenerator based on the features
     * within the region of interest
     * @throws IOException
     */
    private static FeatureGraphGenerator buildFeatureNetwork(SimpleFeatureCollection featureCollection) {
        //create a linear graph generator
        LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();
       
        //wrap it in a feature graph generator
        FeatureGraphGenerator featureGen = new FeatureGraphGenerator(lineStringGen);

        //put all the features into  the graph generator
        SimpleFeatureIterator iter = featureCollection.features();
       
        SimpleFeatureType edgeType = createEdgeFeatureType(featureCollection.getSchema().getCoordinateReferenceSystem());

        GeometryFactory gf = new GeometryFactory(precision);
        //  GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        try {
            while (iter.hasNext()) {
                SimpleFeature feature = iter.next();
                MultiLineString mls = ((MultiLineString) (feature.getDefaultGeometry()));
                for (int i = 0; i < mls.getNumGeometries(); i++) {
                    Coordinate[] coords = ((LineString) mls.getGeometryN(i)).getCoordinates();
                    LineString lineString = gf.createLineString(coords);
                    SimpleFeature segmentFeature = buildFeatureFromGeometry(edgeType, lineString);
                    featureGen.add(segmentFeature);
                }

            }
        } finally {
            iter.close();
        }
        return featureGen;
    }

    private static Node findStartNode(Graph graph, Geometry startLine) {
        for (Node node : (Collection<Node>) graph.getNodes()) {
            if (node.getEdges().size() == 3) {
                for (Edge edge : (List<Edge>) node.getEdges()) {
                    //if (node.getEdges().size() == 1) {
                    //    Edge edge = (Edge)(node.getEdges().get(0));
                    SimpleFeature edgeFeature = (SimpleFeature) edge.getObject();
                    Geometry graphGeom = (Geometry) edgeFeature.getDefaultGeometry();
                    if (graphGeom.buffer(1).contains(startLine)) {
                        LOGGER.info("Found start node");
                        return node;
                    }
                }
            }
        }
        return null;
    }

    private static Node findStartNode(Graph graph, SimpleFeature featureA, SimpleFeature featureB) {
        for (Node node : (Collection<Node>) graph.getNodes()) {
            if (node.getEdges().size() == 2) {
                SimpleFeature edgeFeature1 = (SimpleFeature) (((Edge) node.getEdges().toArray()[0]).getObject());
                SimpleFeature edgeFeature2 = (SimpleFeature) (((Edge) node.getEdges().toArray()[1]).getObject());

                if (edgeFeature1.getID().equals(featureA.getID()) && edgeFeature2.getID().equals(featureB.getID())) {
                    //         LOGGER.info("Found start node edges {},{}", featureA.getDefaultGeometry(),featureB.getDefaultGeometry() );
                    return node;
                }
                if (edgeFeature2.getID().equals(featureA.getID()) && edgeFeature1.getID().equals(featureB.getID())) {
                    //        LOGGER.info("Found start node");
                    return node;
                }
            }
        }
        return null;
    }

    private static void removeFeature(SimpleFeatureCollection networkRegion, Geometry originalLine) {
        SimpleFeatureIterator features = networkRegion.features();
        List<SimpleFeature> newFeatures = new ArrayList();
        while (features.hasNext()) {
            SimpleFeature feature = features.next();
            Geometry geom = (Geometry) feature.getDefaultGeometry();
            if (!(geom.equals(originalLine))) {
                newFeatures.add(feature);
            }
        }
    }

    private static Map graphToMap(Graph graph) {
        Map networkMap = new HashMap();
        for (Node node : (Collection<Node>) graph.getNodes()) {
            //LOGGER.info("Node: " + node);
            for (Edge edge : (List<Edge>) node.getEdges()) {
                //LOGGER.info("Edge: {} Feature: {}",edge.getID(),((SimpleFeature)edge.getObject()).getID());
            }
            networkMap.put(node, node.getEdges());
        }
        return networkMap;
    }

    private static LocationIndexedLine findNearestEdgeLine(SimpleFeatureSource network, Double roadDistance, Double bufferDistance, Point pointOfInterest) throws IOException {
        //Build network Graph - within bounds
        Double maxDistance = roadDistance + bufferDistance;
        SpatialIndex index = createLineStringIndex(network);

        Coordinate pt = pointOfInterest.getCoordinate();
        Envelope search = new Envelope(pt);
        search.expandBy(maxDistance);

        /*
         * Query the spatial index for objects within the search envelope. Note
         * that this just compares the point envelope to the line envelopes so
         * it is possible that the point is actually more distant than
         * MAX_SEARCH_DISTANCE from a line.
         */
        List<LocationIndexedLine> lines = index.query(search);

        // Initialize the minimum distance found to our maximum acceptable
        // distance plus a little bit
        double minDist = maxDistance;// + 1.0e-6;
        Coordinate minDistPoint = null;
        LocationIndexedLine connectedLine = null;

        for (LocationIndexedLine line : lines) {

            LinearLocation here = line.project(pt); //What does project do?
            Coordinate point = line.extractPoint(here); //What does extracPoint do?
            double dist = point.distance(pt);
            if (dist <= minDist) {
                minDist = dist;
                minDistPoint = point;
                connectedLine = line;
            }
        }

        if (minDistPoint != null) {
            //   LOGGER.info("{} - snapped by moving {}\n", pt.toString(), minDist);
            return connectedLine;
        }
        LOGGER.error("Failed to snap point {} to network", pt.toString());
        return null;
    }

    private static SpatialIndex createLineStringIndex(SimpleFeatureSource network) throws IOException {
        SpatialIndex index = new STRtree();

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

        return index;
    }

    private static void writeNetworkFromEdges(Map serviceArea) {
        List<SimpleFeature> features = new ArrayList();
        Set<Edge> edges = serviceArea.keySet();
        for (Edge edge : edges) {
            // SimpleFeature feature = (SimpleFeature) edge.getObject();
            //feature.setDefaultGeometry(serviceArea.get(edge));
            SimpleFeature feature = (SimpleFeature) serviceArea.get(edge);
            features.add(feature);
        }
        try {
            File file = new File("bufferNetwork.json");
            FileUtils.writeStringToFile(file, writeFeatures(DataUtilities.collection(features)));
        } catch (Exception e) {
        }
    }

    /**
     * Generates a buffered service area from a set of network edges
     * @param serviceArea The set of service area edges
     * @param distance the distance to buffer
     * @param crs 
     * @return A buffered service area 
     */
    public static SimpleFeature createBufferFromEdges(Map serviceArea, Double distance, CoordinateReferenceSystem crs) {
        Set<Edge> edges = serviceArea.keySet();
        SimpleFeatureType type = createBufferFeatureType(crs);
        Geometry all = null;
        for (Edge edge : edges) {
            Geometry geom = (Geometry) ((SimpleFeature) serviceArea.get(edge)).getDefaultGeometry();
            geom = geom.getGeometryN(0).buffer(distance);
            try {
                if (all == null) {
                    all = geom;
                } else if (!(all.contains(geom))) {
                    all = all.union(geom);
                }
            } catch (Exception e) {
                LOGGER.info(e.getMessage());
            }
        }
        return buildFeatureFromGeometry(type, all);
    }

    /**
     * Creates a line network representation of service area from set of Edges
     * @param serviceArea The service area edges
     * @return The edges as SimpleFeature
     */
    public static List<SimpleFeature> createLinesFromEdges(Map serviceArea) {
        Set<Edge> edges = serviceArea.keySet();
        List<SimpleFeature> features = new ArrayList();

        for (Edge edge : edges) {
            features.add(((SimpleFeature) serviceArea.get(edge)));
        }
        return features;
    }

    /**
     * Creates a convex hull buffer of service area
     * @param serviceArea The set of edges
     * @param distance The distance to buffer the service area
     * @param type The feature type for the resulting SimpleFeature
     * @return The Convex Hull buffered service area 
     */
    public static SimpleFeature createConvexHullFromEdges(Map serviceArea, Double distance, SimpleFeatureType type) {
        Set<Edge> edges = serviceArea.keySet();
        GeometryCollector gc = new GeometryCollector();
        List<Coordinate> coords = new ArrayList();
        for (Edge edge : edges) {
            Geometry geom = (Geometry) serviceArea.get(edge);
            gc.add(geom);
            Coordinate coordinate = geom.getCoordinate();
            coords.add(coordinate);
        }
        Geometry bufferedConvexHull = gc.collect().convexHull().buffer(distance);
        return buildFeatureFromGeometry(type, bufferedConvexHull);
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

    private static SimpleFeature buildFeatureFromGeometry(SimpleFeatureType featureType, Geometry geom) {

        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(featureType);
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(featureType);
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

    private static SimpleFeatureType createEdgeFeatureType(CoordinateReferenceSystem crs) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Edge");
        builder.setCRS(crs); // <- Coordinate reference system


        // add attributes in order
        builder.add("Edge", LineString.class);
        //  builder.add("Name", String.class); // <- 15 chars width for name field

        // build the type
        return builder.buildFeatureType();
    }

    private static SimpleFeatureType createBufferFeatureType(CoordinateReferenceSystem crs) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Buffer");
        builder.setCRS(crs); // <- Coordinate reference system

        // add attributes in order
        builder.add("Buffer", Polygon.class);
        // builder.add("Name", String.class); // <- 15 chars width for name field

        // build the type
        SimpleFeatureType bufferType = builder.buildFeatureType();

        return bufferType;
    }
}
