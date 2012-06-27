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
package org.mccaughey.spatial;

import java.io.IOException;
import java.net.URL;

import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.mccaughey.utilities.GeoJSONUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author amacaulay
 *
 */
public class IntersectionOMS {

	 static final Logger LOGGER = LoggerFactory.getLogger(IntersectionOMS.class);
	 
	 @In
	 URL regionOfInterest;
	 @In
	 URL featuresOfInterest;
	 @In
	 URL dataStore;
	 @Out
	 URL results;
	 
	 @Execute
	 public void intersection() {
		 try {
			 SimpleFeature roi = GeoJSONUtilities.readFeature(regionOfInterest);
			 SimpleFeatureCollection features = GeoJSONUtilities.readFeatures(featuresOfInterest);
			 FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		     String geometryPropertyName = features.getSchema().getGeometryDescriptor().getLocalName();
			 Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(roi.getDefaultGeometry())); 
			 SimpleFeatureCollection intersectingFeatures = features.subCollection(filter);
			 LOGGER.info("Found {} intersecting features", intersectingFeatures.size());
			 results = GeoJSONUtilities.writeFeatures(intersectingFeatures,dataStore);
		 }
		 catch(IOException e) {
			 LOGGER.equals("Failed to read input datasets");
		 }
	 }
}
