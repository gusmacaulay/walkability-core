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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jsr166y.RecursiveAction;

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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.linearref.LengthIndexedLine;
//import java.util.concurrent.ForkJoinPool;
//import java.util.concurrent.RecursiveAction;

/**
 * A Fork/Join Network service area generator using Bread First graph traversal
 * of a network
 * 
 * @author amacaulay
 */
public class NetworkBufferFJ extends RecursiveAction {

  static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferFJ.class);
  private Map network;
  private Path currentPath;
  private Double distance;
  private Map serviceArea;
  private static String distanceAttribute = "Distance";

  /**
   * Intialise inputs
   * 
   * @param network
   *          Network/graph dataset
   * @param currentPath
   *          The current path being traversed
   * @param distance
   *          The maximum distance to traverse a path
   * @param serviceArea
   *          The service area (set of edges)
   */
  public NetworkBufferFJ(Map network, Path currentPath, Double distance,
      Map serviceArea) {
    this.network = network;
    this.currentPath = currentPath;
    this.distance = distance;
    this.serviceArea = serviceArea;
  }

  /**
   * Sets up the ForkJoinPool and then calls invoke to find service area
   * 
   * @return A complete service area - set of edges that belong to paths with
   *         maximum distance specified
   */
  public Map createBuffer() {
    // Get the available processors, processors==threads is probably best?
    // Runtime runtime = Runtime.getRuntime();
    // int nProcessors = runtime.availableProcessors();
    /*
     * int nThreads = 1;//nProcessors + 1; //
     * LOGGER.debug("Initialising ForkJoinPool with {}", nThreads); //Fork/Join
     * handles threads for me, all I do is invoke try { ForkJoinPool fjPool =
     * new ForkJoinPool(nThreads); fjPool.invoke(this); } catch
     * (NullPointerException e) {
     * LOGGER.error("Failed to invoke buffer computation, {}", e.getMessage());
     * }
     */
    compute();
    return serviceArea;
  }

  @Override
  protected void compute() {
    if (currentPath.size() == 0) {
      return;
    }
    List<Path> nextPaths = new ArrayList();
    // List<NetworkBufferFJ> buffernators = new ArrayList();
    Node current = currentPath.getLast();
    List<Edge> graphEdges = (List<Edge>) network.get(current);

    for (Edge graphEdge : graphEdges) { // (List<Edge>)current.getEdges()) {
      Path nextPath = currentPath.duplicate();// new Path();
      // nextPath.addEdges(currentPath.getEdges());
      if (nextPath.addEdge(graphEdge)) {
        if (pathLength(nextPath) <= distance) { // if path + edge less/equal to
                                                // distance
          addWholeEdge(nextPath, nextPaths, graphEdge);
        } else {// else chop edge, append (path + chopped edge) to list of paths
          addChoppedEdge(graphEdge);
        }
      }
    }
    // LOGGER.info("Nextpaths For path {} - {}",currentPath, nextPaths.size());
    for (Path nextPath : nextPaths) {
      NetworkBufferFJ nbfj = new NetworkBufferFJ(network, nextPath, distance,
          serviceArea);
      // buffernators.add(nbfj);
      this.serviceArea = nbfj.createBuffer();
    }

    // if (buffernators.size() > 0) {
    // invokeAll(buffernators);
    // }
  }

  private void addWholeEdge(Path nextPath, List<Path> nextPaths, Edge graphEdge) {
    if ((addEdge(serviceArea, currentPath, graphEdge)
        && (nextPath.getLast().getDegree() > 1) && nextPath.isValid())) {
      nextPaths.add(nextPath);
    }
    // if (nextPath.isValid()) { //check if valid path (no repeated nodes)
    // if (nextPath.getLast().getDegree() == 1) {
    // addEdge(serviceArea, currentPath, graphEdge); //add the path if it is
    // ended
    //
    // } else if (nextPath.getLast().getDegree() > 1) {//otherwise add to list
    // of paths to explore further
    // if (addEdge(serviceArea, currentPath, graphEdge)) {
    // nextPaths.add(nextPath);
    // }
    // }
    // } else {
    // addEdge(serviceArea, currentPath, graphEdge);
    // }
  }

  private void addChoppedEdge(Edge graphEdge) {
    if (graphEdge.getNodeA().equals(graphEdge.getNodeB())) {

      // looped feature, chopped edges from each direction
      Edge choppedEdgeA = chopEdge(currentPath, graphEdge, distance
          - pathLength(currentPath));
      // graphEdge.reverse();
      Edge choppedEdgeB = chopEdgeBackwards(currentPath, graphEdge, distance
          - pathLength(currentPath));
      Path newPathA = currentPath.duplicate();
      Path newPathB = currentPath.duplicate();
      if (newPathA.addEdge(choppedEdgeA)) {
        addNewEdge(serviceArea, currentPath, graphEdge, choppedEdgeA);
      }
      if (newPathB.addEdge(choppedEdgeB)) {
        addNewEdge(serviceArea, currentPath, graphEdge, choppedEdgeB);
      }
    } else {

      Edge choppedEdge = chopEdge(currentPath, graphEdge, distance
          - pathLength(currentPath));
      // LOGGER.info("Chopped edge should be {} long",distance -
      // pathLength(currentPath));
      Path newPath = currentPath.duplicate();
      if (newPath.addEdge(choppedEdge)) {
        addNewEdge(serviceArea, currentPath, graphEdge, choppedEdge);
      }
      // LOGGER.info("New path length: {}", pathLength(newPath));
    }
  }

  private static boolean addEdge(Map serviceArea, Path path, Edge newEdge) {
    Double pathLength = pathLength(path);
    SimpleFeature graphFeature = ((SimpleFeature) newEdge.getObject());
    SimpleFeatureType edgeType = createEdgeFeatureType(graphFeature.getType()
        .getCoordinateReferenceSystem());
    SimpleFeature edgeFeature = buildFeatureFromGeometry(edgeType,
        (Geometry) graphFeature.getDefaultGeometry());
    if (serviceArea.containsKey(newEdge)) {
      SimpleFeature existingFeature = (SimpleFeature) serviceArea.get(newEdge);
      Double minimalDistance = (Double) existingFeature
          .getAttribute(distanceAttribute);
      if (minimalDistance > pathLength) {
        edgeFeature.setAttribute(distanceAttribute, pathLength);
        serviceArea.put(newEdge, edgeFeature);
        // LOGGER.info("Adding edge {} - minimal distance {}",newEdge,pathLength);
        return true;
      } else {
        // LOGGER.info("NOT Adding edge {} - minimaldistance {}",newEdge,minimalDistance);
        return false;
      }
    } else {
      edgeFeature.setAttribute(distanceAttribute, pathLength);
      serviceArea.put(newEdge, edgeFeature);
      // LOGGER.info("Adding edge {} - minimal distance {}",newEdge,pathLength);
      return true;
    }
  }

  private static void addNewEdge(Map serviceArea, Path path, Edge graphEdge,
      Edge newEdge) {
    Double pathLength = pathLength(path);
    if (serviceArea.containsKey(graphEdge)) {
      SimpleFeature existingFeature = (SimpleFeature) serviceArea
          .get(graphEdge);
      // Double currentPathLength = (Double)
      // existingFeature.getAttribute(distanceAttribute);

      Geometry existingGeometry = (Geometry) existingFeature
          .getDefaultGeometry();
      Geometry newGeometry = (Geometry) ((SimpleFeature) newEdge.getObject())
          .getDefaultGeometry();

      SimpleFeature newFeature = buildFeatureFromGeometry(
          existingFeature.getType(), newGeometry);
      newFeature.setAttribute(distanceAttribute, pathLength);

      if (newGeometry.getLength() >= existingGeometry.getLength()) {
        if (newGeometry.contains(existingGeometry)) {
          serviceArea.put(graphEdge, newFeature);
        } else {
          serviceArea.put(newEdge, newFeature);
        }
      }

    } else {
      SimpleFeature newFeature = (SimpleFeature) newEdge.getObject();
      Geometry newGeometry = (Geometry) newFeature.getDefaultGeometry();
      SimpleFeatureType edgeType = createEdgeFeatureType(newFeature.getType()
          .getCoordinateReferenceSystem());
      newFeature = buildFeatureFromGeometry(edgeType, newGeometry);
      newFeature.setAttribute(distanceAttribute, pathLength);

      serviceArea.put(graphEdge, newFeature);

    }
  }

  private static Edge chopEdge(Path path, Edge edge, Double length) {
    Node node = path.getLast();
    Node newNode = new BasicNode();
    Edge newEdge = new BasicEdge(node, newNode);

    Geometry lineGeom = ((Geometry) ((SimpleFeature) edge.getObject())
        .getDefaultGeometry());
    // lineGeom = Densifier.densify(lineGeom, 1); //1 metre tolerance
    LengthIndexedLine line = new LengthIndexedLine(lineGeom);

    if (node.equals(edge.getNodeA())) {
      Geometry newLine = line.extractLine(line.getStartIndex(), length);
      SimpleFeature newFeature = buildFeatureFromGeometry(
          ((SimpleFeature) edge.getObject()).getType(), newLine);
      newEdge.setObject(newFeature);
      // LOGGER.info("...To {}",newLine.getLength());
      // Double delta = 1500.0 - pathLength(path) - newLine.getLength();
      // LOGGER.info("Delta Length A: " + delta);//(newLine.getLength() -
      // length) );
      return newEdge;
    } else if (node.equals(edge.getNodeB())) {
      Geometry newLine = line.extractLine(line.getEndIndex(), -length);
      SimpleFeature newFeature = buildFeatureFromGeometry(
          ((SimpleFeature) edge.getObject()).getType(), newLine);
      newEdge.setObject(newFeature);
      // LOGGER.info("...To {}",newLine.getLength());
      // Double delta = 1500.0 - pathLength(path) - newLine.getLength();
      // LOGGER.info("Delta Length B: " + delta);//(newLine.getLength() -
      // length) );
      return newEdge;
    } else {
      LOGGER.error("Failed To Cut Edge");
      return null;
    }
  }

  private static Edge chopEdgeBackwards(Path path, Edge edge, Double length) {
    Node node = path.getLast();
    Node newNode = new BasicNode();
    Edge newEdge = new BasicEdge(node, newNode);

    Geometry lineGeom = ((Geometry) ((SimpleFeature) edge.getObject())
        .getDefaultGeometry());
    // lineGeom = Densifier.densify(lineGeom, 1); //1 metre tolerance
    LengthIndexedLine line = new LengthIndexedLine(lineGeom);

    if (node.equals(edge.getNodeA())) {
      Geometry newLine = line.extractLine(line.getEndIndex(), -length);

      SimpleFeature newFeature = buildFeatureFromGeometry(
          ((SimpleFeature) edge.getObject()).getType(), newLine);
      newEdge.setObject(newFeature);
      // LOGGER.info("...To {}",newLine.getLength());
      // Double delta = 1500.0 - pathLength(path) - newLine.getLength();
      // LOGGER.info("Delta Length A: " + delta);//(newLine.getLength() -
      // length) );
      return newEdge;
    } else if (node.equals(edge.getNodeB())) {
      Geometry newLine = line.extractLine(line.getStartIndex(), length);
      SimpleFeature newFeature = buildFeatureFromGeometry(
          ((SimpleFeature) edge.getObject()).getType(), newLine);
      newEdge.setObject(newFeature);
      // LOGGER.info("...To {}",newLine.getLength());
      // Double delta = 1500.0 - pathLength(path) - newLine.getLength();
      // LOGGER.info("Delta Length B: " + delta);//(newLine.getLength() -
      // length) );
      return newEdge;
    } else {
      LOGGER.error("Failed To Cut Edge");
      return null;
    }
  }

  private static Double pathLength(Path path) {
    Double length = 0.0;
    for (Edge edge : (List<Edge>) path.getEdges()) {
      length += ((Geometry) ((SimpleFeature) edge.getObject())
          .getDefaultGeometry()).getLength();
    }
    return length;
  }

  private static SimpleFeature buildFeatureFromGeometry(
      SimpleFeatureType featureType, Geometry geom) {

    SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
    stb.init(featureType);
    SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(featureType);
    sfb.add(geom);

    return sfb.buildFeature(null);
  }

  private static SimpleFeatureType createEdgeFeatureType(
      CoordinateReferenceSystem crs) {

    SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    builder.setName("Edge");
    builder.setCRS(crs); // <- Coordinate reference system

    // add attributes in order
    builder.add("Edge", LineString.class);
    builder.add("Name", String.class); // <- 15 chars width for name field
    builder.add("Distance", Double.class);
    // build the type
    return builder.buildFeatureType();
  }
}
