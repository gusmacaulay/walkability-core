/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import com.vividsolutions.jts.geom.Geometry;
import oms3.annotations.*;
import org.geotools.data.simple.SimpleFeatureSource;

/**
 *
 * @author amacaulay
 */
@Description("Calculates Connectivity for a given network")
public class ConnectivityIndexOMS {

    @In public SimpleFeatureSource featureSource;
    @In public Geometry region;
    @Out public double connectivity;
    
    public void ConnectivityIndexOMS() {
        
    }
     
    @Execute
    public void run() throws Exception {
        connectivity = ConnectivityIndex.connectivity(featureSource, region);
    }
    
    
}
