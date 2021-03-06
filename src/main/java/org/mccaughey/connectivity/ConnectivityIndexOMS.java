/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import java.io.IOException;

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
@Description("Calculates the connectivity for a set of neighbourhoods based on the chosen network data set")
public class ConnectivityIndexOMS {

  static final Logger LOGGER = LoggerFactory
      .getLogger(ConnectivityIndexOMS.class);
  /**
   * The road network to count connections from
   */
  @In
  @Name("Network data set")
  @Description("The network data set to count connections from")
  public SimpleFeatureSource network;
  /**
   * The region if interest
   */
  @In
  @Name("Neighbourhoods")
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
    
    validateInputs();
    
    try {
      SimpleFeatureSource networkSource = network;
      SimpleFeatureSource regionSource = regions;

      ConnectivityIndexFJ cifj = new ConnectivityIndexFJ(networkSource,
          regionSource.getFeatures());
      LOGGER.info("Computing connectivity for {} regions");
      cifj.connectivity();
      results = DataUtilities.source(cifj.getResults());

      LOGGER.info("Completed Connectivity calculation");

    } catch (IOException e) { // Can't do much here because of OMS?
      LOGGER.error(e.getMessage());
      throw new IllegalStateException(e);
    }
  }
  
  private void validateInputs() {

    if (network == null) {
      throw new IllegalArgumentException(
          "Connectivity Index Error: A road network was not provided");
    }

    if (regions == null) {
      throw new IllegalArgumentException(
          "Connectivity Index Error: Regions were not provided by the previous component");
    }
  }
}