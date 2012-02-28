/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import java.io.IOException;
import oms3.annotations.*;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for ConnectivityIndex that provides OMS3 annotations so it can be used in OMS3 workflows, scripts etc.
 * @author amacaulay
 */
@Description("Calculates Connectivity for a given network and region")
public class ConnectivityIndexOMS {
    static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityIndexOMS.class);
    /**
     * The road network to count connections from
     */
    @In public SimpleFeatureSource network;
    /**
     * The region if interest
     */
    @In public SimpleFeature region;
    /**
     * The resulting connectivity
     */
    @Out public SimpleFeature connectivityFeature;
    
    /**
     * Processes the featureSource network and region to calculate connectivity
     * @throws Exception 
     */
    @Execute
    public void connectivity() {
        try {
            LOGGER.info("Calculating connectivity with network {} and region {}",network.getName().toString(),region.getID());
            connectivityFeature = ConnectivityIndex.connectivity(network, region);
        }
        catch(Exception e) { //Can't do much here because of OMS?
            LOGGER.error(e.getMessage());
        }
    }
}
