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
package org.mccaughey.statistics;

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
 * An OMS Wrapper for z-score calculation
 * 
 * @author amacaulay
 */
@Name("zscore")
@Description("For a given list of attributes and a set of features, calculates the z score for each attribute and sums the z scores")
public class ZScoreOMS {

  static final Logger LOGGER = LoggerFactory.getLogger(ZScoreOMS.class);
  /**
   * Attributes to calculate z-score for
   */
  @In
  @Name("Zscore attributes")
  @Description("Attributes to calculate z-score for")
  public List<String> attributes;

  /**
   * Regions of interest (GeoJSON)
   */
  @In
  @Name("Regions of interest")
  public SimpleFeatureSource regionsSource;
  /**
   * Resulting regions with z-scores for each attribute and sum of z-scores
   * across attributes
   */
  @Out
  @Name("Result regions")
  @Description("Resulting regions with z-scores for each attribute and sum of z-scores across attributes")
  public SimpleFeatureSource resultsSource;

  /**
   * For a given list of attributes and a set of features, calculates the z
   * score for each attribute and sums the z scores Reads in the regions layer
   * from given URL, writes out results to resultsURL
   */
  @Execute
  public void sumOfZScores() {
    try {
      FeatureIterator<SimpleFeature> regions = regionsSource.getFeatures()
          .features();

      SimpleFeatureCollection statisticsRegions = ZScore.sumZScores(regions,
          attributes);

      resultsSource = DataUtilities.source(statisticsRegions);

    } catch (IOException e) {
      LOGGER.error("Failed to read input/s");
    }
  }
}
