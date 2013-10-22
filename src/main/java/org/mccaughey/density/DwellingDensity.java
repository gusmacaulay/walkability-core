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
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.mccaughey.utilities.ValidationUtils;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Calculates average density across a set of regions intersecting a region of
 * interest
 * 
 * @author amacaulay
 */
public final class DwellingDensity {

	static final Logger LOGGER = LoggerFactory.getLogger(DwellingDensity.class);

	private DwellingDensity() {
	}

	/**
	 * Calculates average density for regions
	 * 
	 * @param populationSource
	 *            The set of regions with a population attribute
	 * @param regions
	 *            The regions of interest
	 * @param populationAttribute
	 *            The attribute containing the population value
	 * @return
	 */
	public static SimpleFeatureCollection averageDensity(
			SimpleFeatureSource populationSource,
			FeatureIterator<SimpleFeature> regions, String populationAttribute) {
		List<SimpleFeature> densityFeatures = new ArrayList<SimpleFeature>();
		while (regions.hasNext()) {
			SimpleFeature lumFeature = averageDensity(populationSource,
					regions.next(), populationAttribute);
			densityFeatures.add(lumFeature);
		}
		return DataUtilities.collection(densityFeatures);
	}

	private static SimpleFeature averageDensity(
			SimpleFeatureSource dwellingSource, SimpleFeature roi,
			String densityAttribute) {
		try {
			SimpleFeatureIterator subRegions = featuresInRegion(dwellingSource,
					(Geometry) roi.getDefaultGeometry()).features();
			Double totalDensity = 0.0;
			Double totalArea = 0.0;
			int count = 0;
			while (subRegions.hasNext()) {
				SimpleFeature dwelling = subRegions.next();
				Object densityAttrVal = dwelling.getAttribute(densityAttribute);

				int population = ((Number) densityAttrVal).intValue();
				double area = ((Geometry) dwelling.getDefaultGeometry())
						.getArea() / 10000D; // FIXME:
												// Use
												// geotools
												// unit
												// conversion!
				totalArea += area;
				LOGGER.debug("area " + area);
				LOGGER.debug("dwellings" + population);
				if (population != 0) {
					totalDensity += (population / area);
					count++;
				}
			}
			// totalDensity = totalDwellings / totalArea;
			LOGGER.debug("Dwelling regions: " + count);
			if (totalDensity != 0) {
				return buildFeature(roi, totalDensity / count);
			} else {
				return null;//Regions without population have no walkability
			}
		} catch (IOException ioe) {
			LOGGER.error("Error selecting features in region");
			return null;
		}

	}

	private static SimpleFeature buildFeature(SimpleFeature region,
			Double density) {

		SimpleFeatureType sft = (SimpleFeatureType) region.getType();
		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
		stb.init(sft);
		stb.setName("densityFeatureType");
		stb.add("AverageDensity", Double.class);
		SimpleFeatureType landUseMixFeatureType = stb.buildFeatureType();
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(
				landUseMixFeatureType);
		sfb.addAll(region.getAttributes());
		sfb.add(ValidationUtils.isValidDouble(density) ? density : null);
		return sfb.buildFeature(region.getID());
	}

	private static SimpleFeatureCollection featuresInRegion(
			SimpleFeatureSource featureSource, Geometry roi) throws IOException {
		// Construct a filter which first filters within the bbox of roi and
		// then
		// filters with intersections of roi
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		FeatureType schema = featureSource.getSchema();

		String geometryPropertyName = schema.getGeometryDescriptor()
				.getLocalName();

		Filter filter = ff.intersects(ff.property(geometryPropertyName),
				ff.literal(roi));

		// collection of filtered features
		return featureSource.getFeatures(filter);
	}
}
