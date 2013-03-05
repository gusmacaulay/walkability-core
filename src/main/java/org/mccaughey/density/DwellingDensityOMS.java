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

import java.io.IOException;

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

import au.org.aurin.types.AttributeSelector;

/**
 * An OMS Wrapper for Average Density
 * 
 * @author amacaulay
 */
@Name("dwellingdensity")
@Description("Calculates Average Residential or Dwelling Density for a set of Neighbourhoods Based on a Polygon Data Set with Population or Dwelling Counts")
public class DwellingDensityOMS {

  static final Logger LOGGER = LoggerFactory
      .getLogger(DwellingDensityOMS.class);
  /**
   * A URL pointing to a GeoJSON representation of regions with a population
   * attribute
   */
  @In
  @Name("Population dataset")
  @Description("A polygon data set with a population dwelling count attribute")
  public SimpleFeatureSource populationSource;

  /**
   * The attribute to use for population
   */
  @In
  @Name("Population attribute")
  @Description("The attribute containing the population or dwelling counts")
  public AttributeSelector countAttribute;

  /**
   * The input regions to calculate density for
   */
  @In
  @Name("Neighbourhoods")
  public SimpleFeatureSource regionsSource;

  /**
   * The resulting regions with average density calculated
   */
  @Out
  @Name("Resulting regions")
  public SimpleFeatureSource resultsSource;


  /**
   * Reads in the population count layer and regions layer from given URLs,
   * writes out average density results to resultsURL
   */
  @Execute
  public void averageDensity() {
    try {

      FeatureIterator<SimpleFeature> regions = regionsSource.getFeatures()
          .features();

      SimpleFeatureCollection densityRegions = DwellingDensity.averageDensity(
          populationSource, regions, countAttribute.getAttributeId());

      resultsSource = DataUtilities.source(densityRegions);

    } catch (IOException e) {
      LOGGER.error("Failed to read input/s");
      throw new IllegalStateException(e);
    }
  }
}