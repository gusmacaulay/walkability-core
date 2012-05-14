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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.geotools.data.DataUtilities;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public final class ZScore {

    static final Logger LOGGER = LoggerFactory.getLogger(ZScore.class);

    public static SimpleFeatureCollection SumZScores(SimpleFeatureIterator features, List<String> attributes) {

        List<SimpleFeature> results = new ArrayList();

        SummaryStatistics stats = new SummaryStatistics();
        try {
            while (features.hasNext()) {
                SimpleFeature region = features.next();
                stats.addValue((Double) region.getAttribute(attributes.get(0)));
                results.add(buildFeature(region, 0.0));
            }
            for (SimpleFeature region : results) {
                Double raw_score = (Double) region.getAttribute(attributes.get(0));
                Double z_score = (raw_score - stats.getMean()) / stats.getStandardDeviation();
                region.setAttribute("SumZScore", z_score);
                LOGGER.info("Z-score: {}", z_score);
            }
        } finally {
            features.close();
        }

        return DataUtilities.collection(results);
    }

    private static SimpleFeature buildFeature(SimpleFeature region, Double ZScore) {

        SimpleFeatureType sft = (SimpleFeatureType) region.getType();
        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(sft);
        stb.setName("statisticsFeatureType");
        stb.add("SumZScore", Double.class);
        SimpleFeatureType landUseMixFeatureType = stb.buildFeatureType();
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(landUseMixFeatureType);
        sfb.addAll(region.getAttributes());
        sfb.add(ZScore);
        return sfb.buildFeature(region.getID());
    }
}
