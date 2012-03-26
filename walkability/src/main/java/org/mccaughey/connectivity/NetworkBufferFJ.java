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
import com.vividsolutions.jts.linearref.LengthIndexedLine;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public class NetworkBufferFJ extends RecursiveAction {

    static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferFJ.class);
    HashMap network;
    Path currentPath;
    Double distance;
    ConcurrentHashMap serviceArea;
    List<ConcurrentHashMap> results;

    public NetworkBufferFJ(HashMap network, Path currentPath, Double distance, ConcurrentHashMap serviceArea) {
        this.network = network;
        this.currentPath = currentPath;
        this.distance = distance;
        this.serviceArea = serviceArea;
        this.results = new ArrayList();
        // results.add(serviceArea);
    }

    /**
     * Sets up the ForkJoinPool and then calls invoke to calculate connectivity
     * for all regions available
     */
    public void createBuffer() {
        //Get the available processors, processors==threads is probably best?
        Runtime runtime = Runtime.getRuntime();
        int nProcessors = runtime.availableProcessors();
        int nThreads = nProcessors + 1;

        LOGGER.debug("Initialising ForkJoinPool with {}", nThreads);
        //Fork/Join handles threads for me, all I do is invoke
        ForkJoinPool fjPool = new ForkJoinPool(nThreads);
        fjPool.invoke(this);


        if (this.isCompletedAbnormally()) {
            LOGGER.error("ForkJoin connectivity calculation failed: {}", this.getException().toString());
            this.completeExceptionally(this.getException());
        }
    }

    @Override
    protected void compute() {
        if (currentPath.size() == 0) {
            return;
        }
        List<Path> nextPaths = new ArrayList();
        ArrayList<NetworkBufferFJ> buffernators = new ArrayList();

        for (Edge graphEdge : (List<Edge>) network.get(currentPath.getLast())) {
            //      LOGGER.info("Current Node has edges: " + node.getEdges());
            Path nextPath = new Path();
            nextPath.addEdges(currentPath.getEdges());
            if (nextPath.addEdge(graphEdge)) {
                if (pathLength(nextPath) <= distance) { //if path + edge less/equal to distance
                    if (nextPath.isValid()) { //check if valid path (no repeated nodes)
                        if (nextPath.getLast().getDegree() == 1) {
                            serviceArea = addEdges(serviceArea, nextPath); //add the path if it is ended
                        } else if (nextPath.getLast().getDegree() > 1) {//otherwise add to list of paths to explore further
                            nextPaths.add(nextPath);
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
        if (nextPaths.size() > 0) {
            for (Path nextPath : nextPaths) {
                NetworkBufferFJ nbfj = new NetworkBufferFJ(network, nextPath, distance, serviceArea);
                buffernators.add(nbfj);
            }
            invokeAll(buffernators);

//            serviceArea = null;
//            for (NetworkBufferFJ nbfj : buffernators) {
//                if (serviceArea == null) {
//                    serviceArea = nbfj.serviceArea;
//                } else {
//                    serviceArea = joinServiceAreas(serviceArea, nbfj.serviceArea);
//                }
//            }
        }
        results = new ArrayList();
        results.add(serviceArea);
    }

    private static HashMap deepCopy(HashMap serviceArea, HashMap newServiceArea) {
        for (Edge key : (Set<Edge>) serviceArea.keySet()) {
            newServiceArea.put(key, serviceArea.get(key));
        }
        return newServiceArea;
    }

    private static HashMap joinServiceAreas(HashMap serviceAreaA, HashMap serviceAreaB) {

        if (serviceAreaA.size() < serviceAreaB.size()) {
            return joinServiceAreas(serviceAreaB, serviceAreaA);
        }
        for (Edge key : (Set<Edge>) serviceAreaB.keySet()) {
            if (serviceAreaA.containsKey(key)) {
                Geometry geomA = (Geometry) serviceAreaA.get(key);
                Geometry geomB = (Geometry) serviceAreaB.get(key);
                if (geomB.contains(geomA)) {
                    serviceAreaA.put(key, geomB);
                }
            } else {
                serviceAreaA.put(key, serviceAreaB.get(key));
            }
        }

        return serviceAreaA;
    }

    private static HashMap joinServiceAreas(List<HashMap> serviceAreas) {
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

    private static ConcurrentHashMap addEdge(ConcurrentHashMap serviceArea, Edge graphEdge, Edge newEdge) {
        if (serviceArea.containsKey(graphEdge)) {
            Geometry existingGeometry = (Geometry) serviceArea.get(graphEdge);
           // if (graphEdge.isVisited() == false) {
                Geometry newGeometry = (Geometry) ((SimpleFeature) newEdge.getObject()).getDefaultGeometry();
                if (newGeometry.contains(existingGeometry)) {
                    serviceArea.put(graphEdge, newGeometry);
                }
           // }
        } else {
            serviceArea.put(graphEdge, ((SimpleFeature) newEdge.getObject()).getDefaultGeometry());
        }
        return serviceArea;
    }

    private static ConcurrentHashMap addEdges(ConcurrentHashMap serviceArea, Path path, Edge graphEdge, Edge newEdge) {
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
    private static ConcurrentHashMap addEdges(ConcurrentHashMap serviceArea, Path path) {
        for (Edge edge : (List<Edge>) path.getEdges()) {
            Geometry geom = (Geometry) ((SimpleFeature) edge.getObject()).getDefaultGeometry();
           // edge.setVisited(true);
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
           // newEdge.setVisited(false);
            //Double delta = 1500.0 - pathLength(path) - newLine.getLength(); 
            //LOGGER.info("Delta Length A: " + delta);//(newLine.getLength() - length) );
            return newEdge;
        } else if (node.equals(edge.getNodeB())) {
            Geometry newLine = line.extractLine(line.getEndIndex(), -length);
            SimpleFeature newFeature = buildFeatureFromGeometry(((SimpleFeature) edge.getObject()).getType(), newLine);
            newEdge.setObject(newFeature);
           // newEdge.setVisited(false);
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

    private static SimpleFeature buildFeatureFromGeometry(SimpleFeatureType featureType, Geometry geom) {

        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(featureType);
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(featureType);
        sfb.add(geom);

        return sfb.buildFeature(null);
    }
}