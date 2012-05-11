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
package org.mccaughey.density;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.mccaughey.landuse.LandUseMix;
import org.mccaughey.utilities.GeoJSONUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OMS Wrapper for Average Density
 * 
 * @author amacaulay
 */
@Description("Calculates Average Density for a given region population count layer and set of regions of interest")
public class DwellingDensityOMS {
    static final Logger LOGGER = LoggerFactory.getLogger(DwellingDensityOMS.class);
    @In
    URL populationURL;
    @In
    String countAttribute;
    @In
    URL regionsURL;
    @Out
    URL resultsURL;
    
     /**
     * Reads in the population count layer and regions layer from given URLs, writes out average density results to resultsURL
     */
    @Execute
    public void averageDensity() {
        try {
            SimpleFeatureSource populationSource = DataUtilities.source(GeoJSONUtilities.readFeatures(populationURL));
            FeatureIterator<SimpleFeature> regions = GeoJSONUtilities.getFeatureIterator(regionsURL);
            
            SimpleFeatureCollection densityRegions = DwellingDensity.averageDensity(populationSource, regions,countAttribute);
        
            //FIXME: need to get real URL somehow? then write to it instead of file
            File file = new File("landUseMixRegions.geojson");
            GeoJSONUtilities.writeFeatures(densityRegions, file);
            resultsURL = file.toURI().toURL();
        } catch (IOException e) {
            LOGGER.error("Failed to read input/s");
            e.printStackTrace();
        }
    }
}