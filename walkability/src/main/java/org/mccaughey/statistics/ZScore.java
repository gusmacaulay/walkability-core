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

import java.util.List;
import org.apache.commons.math.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * @author amacaulay
 */
public final class ZScore {
    
    static final Logger LOGGER = LoggerFactory.getLogger(ZScore.class);
    
    public static void SumZScores(SimpleFeatureIterator features, List<String> attributes) {
        
        AggregateSummaryStatistics aggregate = new AggregateSummaryStatistics();
        SummaryStatistics stats = new SummaryStatistics();
        try {
            while(features.hasNext()) {
                 stats.addValue((Double)features.next().getAttribute(attributes.get(0)));
            }
            Double raw_score = 0.5;
            Double z_score = (raw_score - stats.getMean())/stats.getStandardDeviation();
            LOGGER.info("Z-score: {}", z_score);
        }
        finally {
            features.close();
        }
        
     //  return features;
    }
}
