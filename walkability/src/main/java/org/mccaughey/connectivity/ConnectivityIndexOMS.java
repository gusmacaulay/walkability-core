/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import oms3.annotations.*;
import org.geotools.data.simple.SimpleFeatureSource;

/**
 *
 * @author amacaulay
 */
@Description("Calculates Connectivity for a given network")
public class ConnectivityIndexOMS {

    @In public SimpleFeatureSource featureSource;
    
    @Execute
    public void run() throws Exception {
        System.out.println(ConnectivityIndex.connections(featureSource));
    }
}
