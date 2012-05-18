/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import java.io.*;
import java.net.URL;
import oms3.annotations.*;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureSource;
import org.mccaughey.utilities.GeoJSONUtilities;
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

    static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityIndexOMS.class);
    /**
     * The road network to count connections from
     */
    @In
    public URL network;
    /**
     * The region if interest
     */
    @In
    public URL regions;
    
    @In
    public URL dataStore;
    /**
     * The resulting connectivity
     */
    @Out
    public URL results;

    /**
     * Processes the featureSource network and region to calculate connectivity
     *
     * @throws Exception
     */
    @Execute
    public void run() {
        try {
            SimpleFeatureSource networkSource = DataUtilities.source(GeoJSONUtilities.readFeatures(network));
            SimpleFeatureSource regionSource = DataUtilities.source(GeoJSONUtilities.readFeatures(regions));

            ConnectivityIndexFJ cifj = new ConnectivityIndexFJ(networkSource, regionSource.getFeatures());
            cifj.connectivity();

         //   File file = new File("connectivity_regions_oms.geojson");
            results = GeoJSONUtilities.writeFeatures(cifj.getResults(), dataStore);
            // FileUtils.writeStringToFile(file, writeFeatures(buffers));
            //results = file.toURI().toURL();
            LOGGER.info(results.toString());
//            System.out.println(results);

        } catch (Exception e) { //Can't do much here because of OMS?
            LOGGER.error(e.getMessage());
        }
    }
}
