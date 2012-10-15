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

import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Name;
import oms3.annotations.Out;

import org.geotools.data.DataUtilities;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OMS Wrapper for Network Buffer generation
 * 
 * @author amacaulay
 */
@Name("netbuffer")
@Description("Generates network service areas for points on a network")
public class NetworkBufferOMS {

  static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferOMS.class);
  /**
   * The road network to count connections from
   */
  @In
  @Name("Road Network")
  @Description("The road network to count connections from")
  public SimpleFeatureSource network;
  /**
   * The points of interest
   */
  @In
  @Name("Points of Interest")
  @Description("Points of Interest")
  public SimpleFeatureSource points;
  /**
   * The network distance for the service areas (maximum walk distance)
   */
  @In
  @Name("Maximum walk distance")
  @Description("The network distance for the service areas (maximum walk distance)")
  public Double distance;
  /**
   * The buffer size
   */
  @In
  @Name("Buffer size")
  @Description("Buffer size")
  public Double bufferSize;

  /**
   * The resulting regions url
   */
  @Out
  @Name("Resulting regions")
  public SimpleFeatureSource regions;

  @Out
  @Name("The original road network")
  public SimpleFeatureSource networkOut;

  /**
   * Reads the input network and point datasets then uses NetworkBufferBatch to
   * generate all the network buffers and writes out to regions URL
   */
  @Execute
  public void run() {

    validateInputs();

    try {
      SimpleFeatureSource networkSource = network;
      SimpleFeatureSource pointsSource = points;

      assert (network.getSchema().getCoordinateReferenceSystem() != null);
      //LOGGER.info("Coordinate Units: {}", network.getSchema()
      //    .getCoordinateReferenceSystem().getCoordinateSystem().getAxis(0)
      //    .getUnit().toString());
      assert (network.getSchema().getCoordinateReferenceSystem()
          .getCoordinateSystem().getAxis(0).getUnit().toString().equals("m"));

      LOGGER.info("Received network data containing {} features",
          networkSource.getCount(new Query()));
      LOGGER.info("Received points data containing {} features",
          pointsSource.getCount(new Query()));

      LOGGER.debug("Points Source CRS: {}", pointsSource.getSchema()
          .getCoordinateReferenceSystem());
      LOGGER.info("Generate network service areas...");
      NetworkBufferBatch nbb = new NetworkBufferBatch(networkSource,
          pointsSource.getFeatures(), distance, bufferSize);
      SimpleFeatureCollection buffers = nbb.createBuffers();

      if (buffers.isEmpty()) {
        throw new IllegalStateException(
            "No buffers were generated. Aborting process");
      }

      // File file = new File("service_areas_oms.geojson");
      regions = DataUtilities.source(buffers);

      // regions = file.toURI().toURL();
      LOGGER.info("Completed Network Service Area Generation");

      networkOut = network;

    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      throw new IllegalStateException(e);
    }
  }

  private void validateInputs() {

    if (network == null) {
      throw new IllegalArgumentException(
          "Network buffer error: A road network was not provided");
    }

    if (points == null) {
      throw new IllegalArgumentException(
          "Network buffer error: A set of points was not provided");
    }

    if (distance == null) {
      throw new IllegalArgumentException(
          "Network buffer error: A walking distance must be provided");
    }

    if (bufferSize == null) {
      throw new IllegalArgumentException(
          "Network buffer error: A buffer size must be provided");
    }
  }
}
