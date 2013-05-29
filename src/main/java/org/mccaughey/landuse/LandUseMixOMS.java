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
import java.util.NoSuchElementException;

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

import au.org.aurin.types.NominalAttributeClassification;

/**
 * An OMS Wrapper for Land Use Mix
 * 
 * @author amacaulay
 */
@Name("landmix")
@Description("Calculates Land Use Mix Measure for a set of neighbourhoods over a land use polygon data set ")
public class LandUseMixOMS {

  static final Logger LOGGER = LoggerFactory.getLogger(LandUseMixOMS.class);
  /**
   * Dataset containing Land Use regions.
   */
  @In
  @Name("Land use polygon data set")
  @Description("Dataset containing land use polygons.")
  public SimpleFeatureSource landUseSource;

  /**
   * The attribute (column) containing the classification categories
   */
  @In
  @Name("Classification attribute")
  @Description("The attribute (column) containing the land use classification categories")
  public NominalAttributeClassification classificationAttribute;

  /**
   * The set of regions to calculate Land Use Mix for
   */
  @In
  @Name("Neighbourhoods")
  public SimpleFeatureSource regionsSource;

  /**
   * The location of the resulting dataset (GeoJSON)
   */
  @Out
  @Name("Result regions")
  public SimpleFeatureSource resultsSource;

  /**
   * Reads in the land use layer and regions layer from given URLs, writes out
   * results to resultsURL
 * @throws Exception 
 * @throws NoSuchElementException 
   */
  @Execute
  public void landUseMixMeasure() throws NoSuchElementException, Exception {
    
    try {
      validateInputs();
      LOGGER.info("Calculating Land Use Mix");
      FeatureIterator<SimpleFeature> regions = regionsSource.getFeatures()
          .features();
      SimpleFeatureSource landUse = landUseSource;
      SimpleFeatureCollection lumRegions = LandUseMix.summarise(landUse,
          regions, classificationAttribute.getValues(), classificationAttribute.getAttributeName());
      resultsSource = DataUtilities.source(lumRegions);
      LOGGER.info("Completed :and Use Mix calculation");
    } catch (IOException e) {
      LOGGER.error("Failed to read input/s: {}", e.getMessage());
      throw new IllegalStateException(e);
    }
  }
  
  /*
   * Clear error messages for invalid inputs
   */
  private void validateInputs() throws IOException {

    if (regionsSource == null) {
      throw new IllegalArgumentException(
          "Land Use Mix error: Inputs regions were not provided by the previous component");
    }

    if (regionsSource == null ) {
      throw new IllegalArgumentException(
          "Land Use Mix error: Regions feature source not provided");
    }
    
    if (landUseSource == null) {
      throw new IllegalArgumentException(
          "Land Use Mix error: A land use dataset was provided");
    }

    if (classificationAttribute == null || classificationAttribute.getValues() == null
        || classificationAttribute.getAttributeName() == null) {
      throw new IllegalArgumentException(
          "Land Use Mix error: Invalid attribute classification");
    }
  }
}