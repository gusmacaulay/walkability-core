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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.linearref.LengthIndexedLine;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.graph.path.Path;
import org.geotools.graph.structure.Edge;
import org.geotools.graph.structure.Node;
import org.geotools.graph.structure.basic.BasicEdge;
import org.geotools.graph.structure.basic.BasicNode;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public class NetworkBufferFJ extends RecursiveAction {

    static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferFJ.class);
    private Map network;
    private Path currentPath;
    private Double distance;
    private Map serviceArea;

    public NetworkBufferFJ(Map network, Path currentPath, Double distance, Map serviceArea) {
        this.network = network;
        this.currentPath = currentPath;
        this.distance = distance;
        this.serviceArea = serviceArea;
    }

    /**
     * Sets up the ForkJoinPool and then calls invoke to calculate connectivity
     * for all regions available
     */
    public Map createBuffer() {
        //Get the available processors, processors==threads is probably best?
        Runtime runtime = Runtime.getRuntime();
        int nProcessors = runtime.availableProcessors();
        int nThreads = 1;//nProcessors + 1;

        LOGGER.debug("Initialising ForkJoinPool with {}", nThreads);
        //Fork/Join handles threads for me, all I do is invoke
        ForkJoinPool fjPool = new ForkJoinPool(nThreads);
        fjPool.invoke(this);


        if (this.isCompletedAbnormally()) {
            LOGGER.error("ForkJoin connectivity calculation failed: {}", this.getException().toString());
            this.completeExceptionally(this.getException());
        }
        return serviceArea;
    }

    @Override
    protected void compute() {
        if (currentPath.size() == 0) {
            return;
        }
        List<Path> nextPaths = new ArrayList();
        List<NetworkBufferFJ> buffernators = new ArrayList();
        Node current = currentPath.getLast();
        List<Edge> graphEdges = (List<Edge>) network.get(current);
        //   if (graphEdges != null) {
  //      LOGGER.info("Current node has {} edges", current.getEdges());
    //    LOGGER.info("Current node has {} graph edges", graphEdges);
        for (Edge graphEdge : graphEdges) { //(List<Edge>)current.getEdges()) {
            Path nextPath = new Path();
            nextPath.addEdges(currentPath.getEdges());
            if (nextPath.addEdge(graphEdge)) {
                if (pathLength(nextPath) <= distance) { //if path + edge less/equal to distance
         //           if (nextPath.isValid()) { //check if valid path (no repeated nodes)
//                        if (nextPath.getLast().getDegree() == 1) {
//                            addEdge(serviceArea, currentPath, graphEdge); //add the path if it is ended
//                        } else if (nextPath.getLast().getDegree() > 1) {//otherwise add to list of paths to explore further
                            if (addEdge(serviceArea, currentPath, graphEdge)) {
                                nextPaths.add(nextPath);
                            }
                          //  else
                             //   LOGGER.info("Did not add Edge {} terminating path", graphEdge);
      //                  }
     //               } //else {
//                        addEdge(serviceArea, currentPath, graphEdge);
//                    }
                } else {//else chop edge, append (path + chopped edge) to list of paths
//                    if (graphEdge.getNodeA().equals(graphEdge.getNodeB())) {
//                        LOGGER.info("HMM this actually happens sometimes");
//                    }
                    Edge choppedEdge = chopEdge(currentPath, graphEdge, distance - pathLength(currentPath));
                    Path newPath = new Path();
                    newPath.addEdges(currentPath.getEdges());

                    if (newPath.addEdge(choppedEdge)) {
                        //addEdges(serviceArea, currentPath);
                        addNewEdge(serviceArea, currentPath, graphEdge, choppedEdge);
                    }
                }
            }
            else {
                LOGGER.info("Failed to add edge to nextpath?");
            }
        }
      //  LOGGER.info("Nextpaths For path {} - {}",currentPath, nextPaths.size());
        for (Path nextPath : nextPaths) {
            NetworkBufferFJ nbfj = new NetworkBufferFJ(network, nextPath, distance, serviceArea);
            buffernators.add(nbfj);
        }
        if (buffernators.size() > 0) {
            invokeAll(buffernators);
        }
//        } else {
//            LOGGER.info("No edges connected to this node?");
//            addEdges(serviceArea, currentPath);
//        }
    }

    private static boolean addEdge(Map serviceArea, Path path, Edge newEdge) {
        Double pathLength = pathLength(path);
        SimpleFeature graphFeature = ((SimpleFeature) newEdge.getObject());
        SimpleFeatureType edgeType = createEdgeFeatureType(graphFeature.getType().getCoordinateReferenceSystem());
        SimpleFeature edgeFeature = buildFeatureFromGeometry(edgeType, (Geometry) graphFeature.getDefaultGeometry());
        if (serviceArea.containsKey(newEdge)) {
            SimpleFeature existingFeature = (SimpleFeature) serviceArea.get(newEdge);
            Double minimalDistance = (Double) existingFeature.getAttribute("Distance");
            if (minimalDistance > pathLength) {
                edgeFeature.setAttribute("Distance", pathLength);
                serviceArea.put(newEdge, edgeFeature);
          //      LOGGER.info("Adding edge {} - minimal distance {}",newEdge,pathLength);
                return true;
            } else {
         //       LOGGER.info("NOT Adding edge {} - minimaldistance {}",newEdge,minimalDistance);
                return false;
            }
        } else {
            edgeFeature.setAttribute("Distance", pathLength);
            serviceArea.put(newEdge, edgeFeature);
        //    LOGGER.info("Adding edge {} - minimal distance {}",newEdge,pathLength);
            return true;
        }
    }

    private static void addNewEdge(Map serviceArea, Path path, Edge graphEdge, Edge newEdge) {
        Double pathLength = pathLength(path);
        if (serviceArea.containsKey(graphEdge)) {
            SimpleFeature existingFeature = (SimpleFeature) serviceArea.get(graphEdge);
            Double currentPathLength = (Double) existingFeature.getAttribute("Distance");
            Geometry existingGeometry = (Geometry) existingFeature.getDefaultGeometry();
            Geometry newGeometry = (Geometry) ((SimpleFeature) newEdge.getObject()).getDefaultGeometry();
            SimpleFeature newFeature = buildFeatureFromGeometry(existingFeature.getType(), newGeometry);
            newFeature.setAttribute("Distance", pathLength);
            serviceArea.put(newEdge, newFeature);
//            if (newGeometry.getLength() > existingGeometry.getLength()) {
//                if (newGeometry.touches(existingGeometry)) {
//                    serviceArea.put(graphEdge, newFeature);
//                    return;
//                }
//            }
//            else if (!(newGeometry.touches(existingGeometry))) {
//                serviceArea.put(newEdge, newFeature);
//            }

        } else {
            SimpleFeature newFeature = (SimpleFeature) newEdge.getObject();
            Geometry newGeometry = (Geometry) newFeature.getDefaultGeometry();
            SimpleFeatureType edgeType = createEdgeFeatureType(newFeature.getType().getCoordinateReferenceSystem());
            newFeature = buildFeatureFromGeometry(edgeType, newGeometry);
            newFeature.setAttribute("Distance", pathLength);
            serviceArea.put(graphEdge, newFeature);
        }
    }

//    private static void addNewEdge(Map serviceArea, Edge graphEdge, Edge newEdge) {
//        if (serviceArea.containsKey(graphEdge)) {
//            Geometry existingGeometry = (Geometry) serviceArea.get(graphEdge);
//            Geometry newGeometry = (Geometry) ((SimpleFeature) newEdge.getObject()).getDefaultGeometry();
//            if (newGeometry.getLength() > existingGeometry.getLength()) {
//                serviceArea.put(graphEdge, newGeometry);
//            }
//        } else {
//            serviceArea.put(graphEdge, ((SimpleFeature) newEdge.getObject()).getDefaultGeometry());
//        }
//    }
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
    private static void addEdges(Map serviceArea, Path path) {
        for (Edge edge : (List<Edge>) path.getEdges()) {
            Geometry geom = (Geometry) ((SimpleFeature) edge.getObject()).getDefaultGeometry();
            serviceArea.put(edge, geom);
        }
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
//            Double delta = 1500.0 - pathLength(path) - newLine.getLength(); 
//            LOGGER.info("Delta Length A: " + delta);//(newLine.getLength() - length) );
            return newEdge;
        } else if (node.equals(edge.getNodeB())) {
            Geometry newLine = line.extractLine(line.getEndIndex(), -length);
            SimpleFeature newFeature = buildFeatureFromGeometry(((SimpleFeature) edge.getObject()).getType(), newLine);
            newEdge.setObject(newFeature);
//            Double delta = 1500.0 - pathLength(path) - newLine.getLength(); 
//            LOGGER.info("Delta Length B: " + delta);//(newLine.getLength() - length) );
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

    private static SimpleFeature buildFeatureFromGeometry(SimpleFeatureType featureType, Geometry geom) {

        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(featureType);
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(featureType);
        sfb.add(geom);

        return sfb.buildFeature(null);
    }

    private static SimpleFeatureType createEdgeFeatureType(CoordinateReferenceSystem crs) {

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("Edge");
        builder.setCRS(crs); // <- Coordinate reference system

        // add attributes in order
        builder.add("Edge", LineString.class);
        builder.length(15).add("Name", String.class); // <- 15 chars width for name field
        builder.add("Distance", Double.class);
        // build the type
        SimpleFeatureType bufferType = builder.buildFeatureType();

        return bufferType;
    }
}