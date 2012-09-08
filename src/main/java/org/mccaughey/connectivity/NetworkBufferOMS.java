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
    public SimpleFeatureSource network;
    /**
     * The points of interest
     */
    @In
    public SimpleFeatureSource points;
    /**
     * The network distance for the service areas (maximum walk distance)
     */
    @In
    public Double distance;
    /**
     * The buffer size
     */
    @In
    public Double bufferSize;
 
    /**
     * The resulting regions url
     */
    @Out
    public SimpleFeatureSource regions;
 

    /**
     * Reads the input network and point datasets then uses NetworkBufferBatch
     * to generate all the network buffers and writes out to regions URL
     */
    @Execute
    public void run() {
        try {

        	LOGGER.info("Reading in network...");
            SimpleFeatureSource networkSource = network;
            LOGGER.info("Reading in points...");
            SimpleFeatureSource pointsSource = points;

            //     LOGGER.info("Points Source CRS: {}", pointsSource.getSchema().getCoordinateReferenceSystem());
            LOGGER.info("Generate network service areas...");
            NetworkBufferBatch nbb = new NetworkBufferBatch(networkSource, pointsSource.getFeatures(), distance, bufferSize);
            SimpleFeatureCollection buffers = nbb.createBuffers();

//            if (buffers.getSchema().getCoordinateReferenceSystem() == null) {
//                LOGGER.error("NULL buffers fail");
//            }


            //  File file = new File("service_areas_oms.geojson");
            regions = DataUtilities.source(buffers);

            //regions = file.toURI().toURL();
            LOGGER.info("Regions uploaded to {}", regions);

        } catch (Exception e) { //Can't do much here because of OMS?
            LOGGER.error(e.getMessage());
            //e.printStackTrace();
        }
    }
}
