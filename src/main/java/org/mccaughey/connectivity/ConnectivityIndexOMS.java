/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Name;
import oms3.annotations.Out;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for ConnectivityIndex that provides OMS3 annotations so it
 * can be used in OMS3 workflows, scripts etc.
 * 
 * @author amacaulay
 */
@Name("connectivity")
@Description("Calculates Connectivity for a given network and region")
public class ConnectivityIndexOMS {

  static final Logger LOGGER = LoggerFactory
      .getLogger(ConnectivityIndexOMS.class);
  /**
   * The road network to count connections from
   */
  @In
  @Name("Road network")
  @Description("The road network to count connections from")
  public SimpleFeatureSource network;
  /**
   * The region if interest
   */
  @In
  @Name("Regions of Interest")
  public SimpleFeatureSource regions;

  /**
   * The resulting connectivity
   */
  @Out
  @Name("Resulting connectivity")
  public SimpleFeatureSource results;

  /**
   * Processes the featureSource network and region to calculate connectivity
   * 
   * @throws Exception
   */
  @Execute
  public void run() {
    try {
      SimpleFeatureSource networkSource = network;
      SimpleFeatureSource regionSource = regions;

      ConnectivityIndexFJ cifj = new ConnectivityIndexFJ(networkSource,
          regionSource.getFeatures());
      cifj.connectivity();

      // File file = new File("connectivity_regions_oms.geojson");
      results = DataUtilities.source(cifj.getResults());
      // FileUtils.writeStringToFile(file, writeFeatures(buffers));
      // results = file.toURI().toURL();

      // System.out.println(results);

    } catch (Exception e) { // Can't do much here because of OMS?
      LOGGER.error(e.getMessage());
    }
  }
}
