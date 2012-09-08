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
package org.mccaughey.landuse;

import java.io.IOException;
import java.util.List;

import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Name;
import oms3.annotations.Out;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OMS Wrapper for Land Use Mix
 *
 * @author amacaulay
 */
@Name("landmix")
@Description("Calculates Land Use Mix Measure for a given land use layer and set of regions")
public class LandUseMixOMS {

    static final Logger LOGGER = LoggerFactory.getLogger(LandUseMixOMS.class);
    /**
     * URL containing geojson representation of Land Use regions.
     */
    @In
    public SimpleFeatureSource landUseSource;
    
    /**
     * A List of classification categories to use in the Land Use Mix calculation
     */
    @In
    public List<String> classifications;
    
    /**
     * The attribute (column) containing the classification categories
     */
    @In
    public String classificationAttribute;
    
    /**
     * The set of regions to calculate Land Use Mix for
     */
    @In
    public SimpleFeatureSource regionsSource;
    
    /**
     * The location of the resulting dataset (GeoJSON)
     */
    @Out
    public SimpleFeatureSource resultsSource;


    /**
     * Reads in the land use layer and regions layer from given URLs, writes out
     * results to resultsURL
     */
    @Execute
    public void landUseMixMeasure() {
        try {
            FeatureIterator<SimpleFeature> regions = regionsSource.getFeatures().features();
            SimpleFeatureSource landUse = landUseSource;
            SimpleFeatureCollection lumRegions = LandUseMix.summarise(landUse, regions, classifications, classificationAttribute);
            resultsSource = DataUtilities.source(lumRegions);
            
        } catch (IOException e) {
            LOGGER.error("Failed to read input/s: {}",e.getMessage());
       //     e.printStackTrace();
        }
    }
}