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

import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.linearref.LengthIndexedLine;
import com.vividsolutions.jts.linearref.LinearLocation;
import com.vividsolutions.jts.linearref.LocationIndexedLine;
import com.vividsolutions.jts.operation.linemerge.LineMerger;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
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
import org.geotools.graph.structure.basic.BasicEdge;
import org.geotools.graph.structure.basic.BasicNode;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
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

    public static SimpleFeature createPolygonBuffer(SimpleFeatureSource network, SimpleFeature pointFeature, Double bufferDistance, Double trimDistance) throws IOException {
        //Build network Graph - within bounds
        Double maxDistance = bufferDistance + trimDistance;
        Point pointOfInterest = (Point) pointFeature.getDefaultGeometry();

        Geometry pointBuffer = pointOfInterest.buffer(maxDistance);

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
        double minDist = maxDistance + 1.0e-6;
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
            LOGGER.info("No Network Feature within trim distance");

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
            // List<Path> paths = findPaths(graph, startPath, bufferDistance);
            HashMap serviceArea = new HashMap();
            HashMap networkHash = new HashMap();
            double t1 = new Date().getTime();
            for (Node node : (Collection<Node>) graph.getNodes()) {
                networkHash.put(node, node.getEdges());
            }


            serviceArea = findPaths(networkHash, startPath, bufferDistance, serviceArea);
//            NetworkBufferFJ nbfj = new NetworkBufferFJ(networkHash, startPath, bufferDistance, serviceArea);
//            nbfj.createBuffer();
//            double t2 = new Date().getTime();
//            double total = (t2 - t1) / 1000;
//            LOGGER.info("Created unjoined buffer graph in " + total + " seconds");
//            serviceArea = nbfj.serviceArea;//joinServiceAreas(nbfj.results);
//            double t3 = new Date().getTime();
//            total = (t3 - t2) / 1000;
//            LOGGER.info("Found " + serviceArea.size() + " Edges in " + total + " seconds");





//            LOGGER.info("Found paths: " + String.valueOf(paths.size()));
            SimpleFeatureType type = createFeatureType(pointFeature.getFeatureType().getCoordinateReferenceSystem());
//            LOGGER.info("Buffering paths ...");
//            return createBufferFromPaths(paths, trimDistance, type);
            //return createBufferFromEdges(serviceArea,trimDistance,type);
            return createBufferFromEdges(serviceArea, trimDistance, type);
        }
        return null;
    }

    private static HashMap joinServiceAreas(List<HashMap> serviceAreas) {
        if (serviceAreas.size() == 1) {
            return serviceAreas.get(1);
        }
        HashMap serviceArea = serviceAreas.get(0);

        for (HashMap otherArea : serviceAreas.subList(1, serviceAreas.size())) {
            Set<Edge> keys = otherArea.keySet();
            for (Edge key : keys) {
                if (serviceArea.containsKey(key)) {
                    Geometry geomA = (Geometry) otherArea.get(key);
                    Geometry geomB = (Geometry) serviceArea.get(key);
                    if (geomA.contains(geomB)) {
                        serviceArea.put(key, geomA);
                    }
                } else {
                    serviceArea.put(key, otherArea.get(key));
                }
            }
        }
        return serviceArea;
    }

    private static SimpleFeatureType createFeatureType(CoordinateReferenceSystem crs) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Buffer");
        builder.setCRS(crs); // <- Coordinate reference system

        // add attributes in order
        builder.add("Buffer", Polygon.class);
        builder.length(15).add("Name", String.class); // <- 15 chars width for name field

        // build the type
        final SimpleFeatureType BUFFER = builder.buildFeatureType();

        return BUFFER;
    }

    private static SimpleFeature createConvexHullFromEdges(HashMap serviceArea, Double distance, SimpleFeatureType type) {
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

    private static SimpleFeature createBufferFromEdges(HashMap serviceArea, Double distance, SimpleFeatureType type) {
        Set<Edge> edges = serviceArea.keySet();

        Geometry all = null;
        for (Edge edge : edges) {
            Geometry geom = (Geometry) serviceArea.get(edge);

            try {
                if (all == null) {
                    all = geom.getGeometryN(0).buffer(distance);
                } else if (!(all.contains(geom.getGeometryN(0).buffer(distance)))) {
                    all = all.union(geom.getGeometryN(0).buffer(distance));
                }
            } catch (Exception e) {
            }
        }
        return buildFeatureFromGeometry(type, all);
    }

    private static SimpleFeature createBufferFromPaths(List<Path> paths, Double distance, SimpleFeatureType type) {
        LineMerger lineMerger = new LineMerger();
        List<SimpleFeature> features = new ArrayList();
        for (Path path : paths) {
            // LOGGER.info("Path: " + path.getEdges());
            for (Edge edge : (List<Edge>) path.getEdges()) {
                features.add(((SimpleFeature) edge.getObject()));
            }
        }
        SimpleFeatureCollection fc = DataUtilities.collection(features);
        File file = new File("/home/amacaulay/buffers.json");
        try {
            FileUtils.writeStringToFile(file, writeFeatures(fc));
        } catch (Exception e) {
        }
        LOGGER.info("Unique Features?: " + fc.size());
        SimpleFeatureIterator featureIter = fc.features();
        Geometry all = null;
        List<Geometry> geometries = new ArrayList();
        List<Geometry> containedGeometries = new ArrayList();
        while (featureIter.hasNext()) {
            Geometry geom = ((Geometry) featureIter.next().getDefaultGeometry()).getGeometryN(0);
            Boolean redundantGeom = false;
            for (Geometry otherGeom : geometries) {
                if (geom.contains(otherGeom)) {
                    containedGeometries.add(otherGeom);
                } else if (otherGeom.contains(geom)) {
                    redundantGeom = true;
                }
            }
            for (Geometry removeGeom : containedGeometries) {
                geometries.remove(removeGeom);
            }
            if (!redundantGeom) {
                geometries.add(geom);
            }
        }
        LOGGER.info("Remaining Geometrires: " + geometries.size());
        for (Geometry geom : geometries) {
            try {
                if (all == null) {
                    all = geom.getGeometryN(0).buffer(distance);
                } else if (!(all.contains(geom.getGeometryN(0).buffer(distance)))) {
                    all = all.union(geom.getGeometryN(0).buffer(distance));
                }
            } catch (Exception e) {
            }
        }

        return buildFeatureFromGeometry(type, all);
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

    private static HashMap findPaths(HashMap network, Path currentPath, Double distance, HashMap serviceArea) {
        //List<Path> paths = new ArrayList();
        if (currentPath.size() == 0) {
            //serviceArea.addNode(currentPath.getFirst());
            return serviceArea;
        }
        for (Edge graphEdge : (List<Edge>) network.get(currentPath.getLast())) {
            //      LOGGER.info("Current Node has edges: " + node.getEdges());
            Path nextPath = new Path();
            nextPath.addEdges(currentPath.getEdges());
            if (nextPath.addEdge(graphEdge)) {
                if (pathLength(nextPath) <= distance) { //if path + edge less/equal to distance
                    if (nextPath.isValid()) { //check if valid path (no repeated nodes)
                        if (nextPath.getLast().getDegree() == 1) {
                            serviceArea = addEdges(serviceArea, nextPath);
                            // serviceArea.put(graphEdge,((SimpleFeature)graphEdge.getObject()).getDefaultGeometry()); //add the path if it is ended
                        } else if (nextPath.getLast().getDegree() > 1) {//otherwise explore the path further
                            serviceArea = findPaths(network, nextPath, distance, serviceArea);
                        }
                    } else if (nextPath.isClosed()) {//if the path happens to be invalid but is a closed walk then still add it - don't want to miss out on looped edges
                        serviceArea = addEdges(serviceArea, nextPath);
                    }
                } else {//else chop edge, append (path + chopped edge) to list of paths
                    Edge choppedEdge = chopEdge(currentPath, graphEdge, distance - pathLength(currentPath));
                    Path newPath = new Path();
                    newPath.addEdges(currentPath.getEdges());

                    if (newPath.addEdge(choppedEdge)) {
                        //LOGGER.info("Path Length: " + pathLength(newPath));
                        if (newPath.isValid()) {
                            serviceArea = addEdges(serviceArea, currentPath, graphEdge, choppedEdge);

                        } else if (newPath.isClosed()) {//if the path happens to be invalid but is a closed walk then still add it - don't want to miss out on looped edges
                            serviceArea = addEdges(serviceArea, currentPath, graphEdge, choppedEdge);
                        }
                    }
                }
            }
        }
        return serviceArea;
    }

    private static HashMap addEdge(HashMap serviceArea, Edge graphEdge, Edge newEdge) {
        if (serviceArea.containsKey(graphEdge)) {
            Geometry existingGeometry = (Geometry) serviceArea.get(graphEdge);
            Geometry newGeometry = (Geometry) ((SimpleFeature) newEdge.getObject()).getDefaultGeometry();
            if (newGeometry.contains(existingGeometry)) {
                serviceArea.put(graphEdge, newGeometry);
            }
        } else {
            serviceArea.put(graphEdge, ((SimpleFeature) newEdge.getObject()).getDefaultGeometry());
        }
        return serviceArea;
    }

    private static HashMap addEdges(HashMap serviceArea, Path path, Edge graphEdge, Edge newEdge) {
        serviceArea = addEdges(serviceArea, path);
        serviceArea = addEdge(serviceArea, graphEdge, newEdge);
        return serviceArea;
    }

    /**
     * This method adds all the edges in the path to the serviceArea HashMap It
     * assumes that the path is made up of complete graph edge geometries so
     * does not check for sub-geometries before adding to the hash map.
     *
     * @param serviceArea Represents the set of unique edges from all paths in
     * the service area
     * @param path a service area path which is made up of complete graph edge
     * geometries.
     * @return
     */
    private static HashMap addEdges(HashMap serviceArea, Path path) {
        for (Edge edge : (List<Edge>) path.getEdges()) {
            Geometry geom = (Geometry) ((SimpleFeature) edge.getObject()).getDefaultGeometry();
            serviceArea.put(edge, geom);
        }
        return serviceArea;
    }

    private static Edge chopEdge(Path path, Edge edge, Double length) {
        Node node = path.getLast();
        Node newNode = new BasicNode();
        Edge newEdge = new BasicEdge(node, newNode);

        Geometry lineGeom = ((Geometry) ((SimpleFeature) edge.getObject()).getDefaultGeometry());
        //lineGeom = Densifier.densify(lineGeom, 0.1); //0.1 metre tolerance
        LengthIndexedLine line = new LengthIndexedLine(lineGeom);

        if (node.equals(edge.getNodeA())) {
            Geometry newLine = line.extractLine(line.getStartIndex(), length);
            SimpleFeature newFeature = buildFeatureFromGeometry(((SimpleFeature) edge.getObject()).getType(), newLine);
            newEdge.setObject(newFeature);
            //Double delta = 1500.0 - pathLength(path) - newLine.getLength(); 
            //LOGGER.info("Delta Length A: " + delta);//(newLine.getLength() - length) );
            return newEdge;
        } else if (node.equals(edge.getNodeB())) {
            Geometry newLine = line.extractLine(line.getEndIndex(), -length);
            SimpleFeature newFeature = buildFeatureFromGeometry(((SimpleFeature) edge.getObject()).getType(), newLine);
            newEdge.setObject(newFeature);
            //Double delta = 1500.0 - pathLength(path) - newLine.getLength(); 
            // LOGGER.info("Delta Length B: " + delta);//(newLine.getLength() - length) );
            return newEdge;
        } else {
            LOGGER.error("Failed To Cut Edge");
            return null;
        }
    }

    private static Double pathLength(Path path) {
        Double length = 0.0;
        for (Edge edge : (List<Edge>) path.getEdges()) {
            length += ((Geometry) ((SimpleFeature) edge.getObject()).getDefaultGeometry()).getLength();
        }
        return length;
    }

    private static String graphToJSON(Graph graph) {
        List<SimpleFeature> features = new ArrayList();

        for (Edge edge : (Collection<Edge>) graph.getEdges()) {
            features.add(((SimpleFeature) edge.getObject()));
        }
        return (writeFeatures(DataUtilities.collection(features)));
    }

    private static String pathsToJSON(List<Path> paths) {
        List<SimpleFeature> features = new ArrayList();
        for (Path path : paths) {
            // LOGGER.info("Path: " + path.getEdges());
            for (Edge edge : (List<Edge>) path.getEdges()) {
                features.add(((SimpleFeature) edge.getObject()));
            }
            //LOGGER.info(writeFeatures(DataUtilities.collection(features)));    
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
