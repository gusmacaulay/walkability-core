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
import org.geotools.graph.structure.Edge;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.Node;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
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
        LocationIndexedLine connectedLine = null;

        for (LocationIndexedLine line : lines) {

            LinearLocation here = line.project(pt); //What does project do?
            Coordinate point = line.extractPoint(here); //What does extracPoint do?
            double dist = point.distance(pt);
            if (dist < minDist) {
                minDist = dist;
                minDistPoint = point;
                connectedLine = line;
            }
        }

        if (minDistPoint == null) {
            // No line close enough to snap the point to
            LOGGER.info(pt + "- X");

        } else {
            LOGGER.info("{} - snapped by moving {}\n", pt.toString(), minDist);


            //networkGraphGen.remove(buildFeatureFromGeometry(networkRegion.getSchema(),connectedLine.extractLine(connectedLine.getStartIndex(), connectedLine.getEndIndex())));
            Geometry lineA = connectedLine.extractLine(connectedLine.getStartIndex(), connectedLine.project(minDistPoint));
            Geometry lineB = connectedLine.extractLine(connectedLine.project(minDistPoint), connectedLine.getEndIndex());

//            pointFeature.setDefaultGeometry(pointOfInterest);
//            GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
//            Coordinate[] coords = new Coordinate[]{new Coordinate(pt.x, pt.y), new Coordinate(minDistPoint.x, minDistPoint.y)};
//            LineString snapLine = geometryFactory.createLineString(coords);
//           SimpleFeature feature =  buildFeatureFromGeometry(networkRegion.getSchema(), snapLine);

            System.out.println("Orginal nodes: " + String.valueOf(networkRegion.size()));
            FeatureGraphGenerator networkGraphGen = buildFeatureNetwork(networkRegion);
            SimpleFeature featureA = buildFeatureFromGeometry(networkRegion.getSchema(), lineA);
            networkGraphGen.add(featureA);
            SimpleFeature featureB = buildFeatureFromGeometry(networkRegion.getSchema(), lineB);
            networkGraphGen.add(featureB);
            Graph graph = networkGraphGen.getGraph();
            System.out.println("New nodes: " + String.valueOf(graph.getEdges().size()));

            Path startPath = new Path();
            // Node startNode = (Node) graph.getNodes().toArray()[0]; //hmm is the snap node the last node?
            //startPath.add(startNode);
            for (Node node : (Collection<Node>) graph.getNodes()) {
                if (node.getEdges().size() == 2) {
                    SimpleFeature edgeFeature1 = (SimpleFeature) (((Edge) node.getEdges().toArray()[0]).getObject());
                    SimpleFeature edgeFeature2 = (SimpleFeature) (((Edge) node.getEdges().toArray()[1]).getObject());

                    if (edgeFeature1.getID().equals(featureA.getID()) && edgeFeature2.getID().equals(featureB.getID())) {
                        LOGGER.info("Found start node " + node.getEdges());
                        startPath.add(node);
                        break;
                    }
                    if (edgeFeature2.getID().equals(featureA.getID()) && edgeFeature1.getID().equals(featureB.getID())) {
                        LOGGER.info("Found start node " + node.getEdges());
                        startPath.add(node);
                        break;
                    }
                }
            }
            List<Path> paths = findPaths(graph, startPath);
            LOGGER.info("Found paths: " + String.valueOf(paths.size()));
            LOGGER.info(pathsToJSON(paths));
            //  LOGGER.info(graphToJSON(graph));
        }
        return pointFeature;
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
                LOGGER.info("Found Current Node in Graph");
                for (Edge graphEdge : (List<Edge>) node.getEdges()) {
                    LOGGER.info("Current Node has edges: " + node.getEdges());
                    if (currentPath.size() < 8) { //if path + edge less than distance

                        Path nextpath = new Path();
                        nextpath.addEdges(currentPath.getEdges());
                        if (nextpath.addEdge(graphEdge)) {
                            LOGGER.info("Appended edge to path: " + nextpath.getEdges());
                            if (nextpath.isValid()) //check if valid path (no repeated nodes)
                            {   //append findPaths(path+edge) to list of paths
                                paths.addAll(findPaths(network, nextpath));
                                LOGGER.info("Adding edge: " + graphEdge + " ...exploring further");
                            } else if (nextpath.isClosed()) {  //if the path happens to be a closed path then still add it - don't want to miss out on looped edges - but no need to explore path further
                                paths.add(nextpath);
                                LOGGER.info("Adding edge: " + graphEdge + " ...terminating closed walk");
                            }
                        } else {
                            LOGGER.info("Failed to append edge to path");
                        }
                    } else {//else chop edge, append (path + chopped edge) to list of paths
                        Path newpath = new Path();
                        newpath.addEdges(currentPath.getEdges());
                        if (newpath.addEdge(graphEdge)) {
                            LOGGER.info("Appended edge to path: " + newpath.getEdges());
                            if (newpath.isValid()) {
                                paths.add(newpath);
                                LOGGER.info(" ...terminating completed path" + newpath.getEdges());
                            } else if (newpath.isClosed()) {  //if the path happens to be a closed path then still add it - don't want to miss out on looped edges
                                paths.add(newpath);
                                LOGGER.info(" ...terminating completed closed walk: " + newpath.getEdges());
                            }
                        } else {
                            LOGGER.info("Failed to append edge to path");
                        }
                    }
                }
                //return all paths
               // return paths;
            }
        }
        paths.add(currentPath);
        return paths;
        //return null;
    }

    private static String graphToJSON(Graph graph) {
        String json = "";
        List<SimpleFeature> features = new ArrayList();

        for (Edge edge : (Collection<Edge>) graph.getEdges()) {
            features.add(((SimpleFeature) edge.getObject()));
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
