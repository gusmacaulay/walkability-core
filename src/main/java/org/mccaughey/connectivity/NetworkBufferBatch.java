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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.Geometries;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

/**
 * Generates network buffers for a set of points, using Fork/Join for concurrency
 * 
 * @author amacaulay
 */
public class NetworkBufferBatch { // extends RecursiveAction {

  // /**
  // * Some nonsense to satisfy sonar+findbugs
  // */
  // private static final long serialVersionUID = 1L;
  static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferBatch.class);
  private SimpleFeatureSource network;
  private SimpleFeatureCollection points;
  private DefaultFeatureCollection buffers;
  private DefaultFeatureCollection graphs;
  private Double distance;
  private Double bufferSize;
  private int pointsPerThread;

  /**
   * Generates network buffers for a set of points
   * 
   * @param network
   *          The network to use to generate service networks
   * @param points
   *          The set of points of interest
   * @param distance
   *          The distance to traverse along the network.
   * @param bufferSize
   *          The length to buffer the service network
   */
  public NetworkBufferBatch(SimpleFeatureSource network, SimpleFeatureCollection points, Double distance,
      Double bufferSize) {
    this.network = network;
    this.points = points;
    this.distance = distance;
    this.bufferSize = bufferSize;
    this.buffers = new DefaultFeatureCollection();
    this.graphs = new DefaultFeatureCollection();
    this.pointsPerThread = 1000; // TODO: make this dynamic
  }

  /**
   * 
   * @return A SimpleFeatureCollection of the service area networks for all points of interest
   */
  public SimpleFeatureCollection getGraphs() {
    return graphs;
  }

  /**
   * 
   * @return A SimpleFeatureCollection consisting of the buffered service areas for each point of interest
   * @throws IOException
   */
  public SimpleFeatureCollection createBuffers() throws IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    try {
      List<Future> futures = new ArrayList<Future>();
      int count = 0;
      SimpleFeatureIterator features = points.features();
      while (features.hasNext()) {
        for (SimpleFeature point : getIndividualPoints(features.next())) {
          LOGGER.debug("Feature {} : Buffer count {}", point.getID(), ++count);
          // SimpleFeature point = features.next();
          Buffernator ac = new Buffernator(point, network);
          Future future = executorService.submit(ac);
          futures.add(future);
        }
      }
      for (Future future : futures) {
        try {
          buffers.add((SimpleFeature) (future.get()));
          LOGGER.debug("Completing Buffer");
        } catch (ExecutionException e) {
          LOGGER.error("Buffer generation failed for a point", e);
        }
      }
      LOGGER.debug("Completed {} buffers for {} points", buffers.size(), points.size());
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    } finally {
      executorService.shutdownNow();
    }

    return buffers;
  }

  private List<SimpleFeature> getIndividualPoints(SimpleFeature geometryFeature) throws IOException {
    List<SimpleFeature> points = new ArrayList<SimpleFeature>();
    points.addAll(getPointFeatures(geometryFeature));
    return points;
  }

  private Collection<? extends SimpleFeature> getPointFeatures(SimpleFeature geometryFeature) throws IOException {
    List<Point> points = new ArrayList();
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry geometry = (Geometry) geometryFeature.getDefaultGeometry();
    switch (Geometries.get(geometry)) {
      case POINT:
        points.add((Point) geometry);
        List<SimpleFeature> features = new ArrayList();
        features.add(geometryFeature);
        return features;
      case MULTIPOINT:
        Coordinate[] coords = geometry.getCoordinates();
        for (Coordinate coord : coords) {
          points.add(geometryFactory.createPoint(coord));
        }
        return createFeatures(points, geometryFeature);
      case POLYGON:
        Polygon polygon = (Polygon) geometry;
        points.add(polygon.getCentroid());
        return createFeatures(points, geometryFeature);
      case MULTIPOLYGON:
        MultiPolygon multi = (MultiPolygon) geometry;
        for (int n = 0; n < multi.getNumGeometries(); n++) {
          Polygon single = (Polygon) multi.getGeometryN(n);
          points.add(single.getCentroid());
        }
        return createFeatures(points, geometryFeature);
      default:
        LOGGER.error("Geometry type issue, did not expect type: *", geometry.getGeometryType() + "*");
        throw new IOException("Geometry must be one of point,multipoint,polygon");
    }

  }

  private Collection<? extends SimpleFeature> createFeatures(List<Point> pointGeometries, SimpleFeature geometryFeature) {
    List<SimpleFeature> pointFeatures = new ArrayList<SimpleFeature>();
    SimpleFeatureType ft = createPointType(geometryFeature.getType());
    SimpleFeatureBuilder builder = new SimpleFeatureBuilder(ft);
    int sub_id = 0;
    for (Point point : pointGeometries) {

      // add the attributes
      
      builder.addAll(geometryFeature.getAttributes());
      builder.add(point);
      // build the feature
      pointFeatures.add(builder.buildFeature(geometryFeature.getID() + "_" + sub_id++));
    }
    return pointFeatures;
  }

  private SimpleFeatureType createPointType(SimpleFeatureType type) {
    // create the builder
    SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();

    // set global state
    builder.setName("single point");

    // add attributes
    for(AttributeDescriptor ad : type.getAttributeDescriptors()) {
      if (ad.getName() != type.getGeometryDescriptor()) {
        builder.add(ad);
      }
    }
    builder.add("geometry", Point.class);

    // build the type
    return builder.buildFeatureType();

  }

  class Buffernator implements Callable<SimpleFeature> {
    private SimpleFeature point;
    private SimpleFeatureSource network;

    Buffernator(SimpleFeature point, SimpleFeatureSource network) {
      this.point = point;
      this.network = network;
    }

    public SimpleFeature call() throws IOException {
      LOGGER.debug("Calculating service network");
      Map serviceArea = NetworkBuffer.findServiceArea(network, point, distance, bufferSize);

      LOGGER.debug("Buffering service network");
      SimpleFeature networkBuffer = NetworkBuffer.createBufferFromEdges(serviceArea, bufferSize, point,
          String.valueOf(point.getID()));
      // if (networkBuffer != null) {
      return networkBuffer;
    }
  }
}
