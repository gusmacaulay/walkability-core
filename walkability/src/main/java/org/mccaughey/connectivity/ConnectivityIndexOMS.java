/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import oms3.annotations.*;
import org.geotools.data.simple.SimpleFeatureSource;

/**
 * This is a wrapper for ConnectivityIndex that provides OMS3 annotations so it can be used in OMS3 workflows, scripts etc.
 * @author amacaulay
 */
@Description("Calculates Connectivity for a given network and region")
public class ConnectivityIndexOMS {

    /**
     * The road network to count connections from
     */
    @In public SimpleFeatureSource network;
    /**
     * The region if interest
     */
    @In public Geometry region;
    /**
     * The resulting connectivity
     */
    @Out public double connectivity;
    
    /**
     * Processes the featureSource network and region to calculate connectivity
     * @throws Exception 
     */
    @Execute
    public void connectivity() {
        try {
            connectivity = ConnectivityIndex.connectivity(network, region);
        }
        catch(IOException e) { //Can't do much here because of OMS?
            return; //TODO: Add logging
        }
    }
}
