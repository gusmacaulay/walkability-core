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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.mccaughey.utilities.GeoJSONUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.TopologyException;

/**
 * Calculates the Land Use Mix Measure for a given land use dataset, region/s,
 * and a list of classifications of interest
 * 
 * @author amacaulay
 */
public final class LandUseMix {

	static final Logger LOGGER = LoggerFactory.getLogger(LandUseMix.class);
	private static String AttributePrefix = "LUM_";

	// private static List<SimpleFeature> allClippedParcels = new ArrayList();

	private LandUseMix() {
	}

	/**
	 * Calculates Land Use Mix Measure for a set of regions
	 * 
	 * @param landUse
	 *            The Land Use layer containing polygonal features with land use
	 *            classifications
	 * @param regions
	 *            A set of regions to calculate Land Use Mix for
	 * @param classifications
	 *            A List of classifications of interest
	 * @return The set of regions augmented with summary of classification areas
	 *         and land use mix measure as attributes
	 * @throws Exception
	 * @throws NoSuchElementException
	 */
	public static SimpleFeatureCollection summarise(
			SimpleFeatureSource landUse,
			FeatureIterator<SimpleFeature> regions,
			List<String> classifications, String classificationAttribute) {
		List<SimpleFeature> lumFeatures = new ArrayList<SimpleFeature>();
		while (regions.hasNext()) {
			// LOGGER.debug("Summarising Land Use ...");
			SimpleFeature lumFeature = summarise(landUse, regions.next(),
					classifications, classificationAttribute);
			lumFeatures.add(lumFeature);
		}
		// GeoJSONUtilities.writeFeatures(DataUtilities.collection(allClippedParcels),
		// new File("clippedLandUse" + allClippedParcels.hashCode() + ".json"));
		return DataUtilities.collection(lumFeatures);
	}

	/**
	 * Calculates Land Use Mix Measure for a single region
	 * 
	 * @param landUse
	 *            The Land Use layer containing polygonal features with land use
	 *            classifications
	 * @param region
	 *            A region to calculate Land Use Mix for
	 * @param classifications
	 *            A List of classifications of interest
	 * @return The region feature augmented with summary of classification areas
	 *         and land use mix measure as attributes
	 * @throws Exception
	 */
	public static SimpleFeature summarise(SimpleFeatureSource landUse,
			SimpleFeature region, List<String> classifications,
			String classificationAttribute) {

		Map<String, String> classificationsAttributesMap = getSubClassifications(classifications);

		try {
			Geometry regionGeom = ((Geometry) region.getDefaultGeometry()).buffer(0);
			
			SimpleFeatureIterator parcels = trimFeaturesToRegion(
					(featuresInRegion(landUse, regionGeom)), regionGeom)
					.features();
			Map classificationAreas = new HashMap();
			double totalArea = 0.0;
			while (parcels.hasNext()) {
				SimpleFeature parcel = parcels.next();
				try {
					Geometry parcelGeom = ((Geometry) parcel
							.getDefaultGeometry()).buffer(0);
					String subClassification = String.valueOf(parcel
							.getAttribute(classificationAttribute));

					//LOGGER.debug("Classification: {}", subClassification);
					if (classificationsAttributesMap
							.containsKey((subClassification))) {
						String classification = classificationsAttributesMap
								.get(subClassification);
						//LOGGER.debug("Classification: {}", classification);
						Double parcelArea = parcelGeom.intersection(regionGeom)
								.getArea();
						//	LOGGER.debug("Parcel Area:, {}", parcelArea);
						totalArea += parcelArea;
						Double area = parcelArea;
						if (classificationAreas.containsKey(classification)) {
							area = (Double) classificationAreas
									.get(classification) + area;
						}
						classificationAreas.put(classification, area);
					}
				} catch (TopologyException e1) {
					LOGGER.debug("Ignoring TopologyException, {}",
							e1.getMessage());
				}
			}

			Collection<Double> areas = classificationAreas.values();
			SimpleFeatureType sft = (SimpleFeatureType) region.getType();
			SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
			stb.init(sft);
			stb.setName("landUseMixFeatureType");

			for (String classification : classifications) {
				stb.add(AttributePrefix + classification.replace("+", "_and_"),
						Double.class);
			}
			// Add the land use mix attribute
			stb.add("LandUseMixMeasure", Double.class);
			SimpleFeatureType landUseMixFeatureType = stb.buildFeatureType();
			SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(
					landUseMixFeatureType);
			sfb.addAll(region.getAttributes());
			for (String classification : classifications) {
				Double area = (Double) classificationAreas.get(classification);
				if (area == null) {
					area = 0d;
				}
				sfb.add(area);
			}
			Double landUseMixMeasure = calculateLUM(areas, totalArea,
					classifications.size());
			sfb.add(landUseMixMeasure);
			return sfb.buildFeature(region.getID());
			// LOGGER.debug("Land Use Mix Measure: {}", landUseMixMeasure);
		} catch (IOException e) {
			LOGGER.error("Failed to select land use features in region: {}",
					e.getMessage());
			// File file = new File("landUseMixRegions.geojson");
			return null;
		}
		// return region;
	}

	private static Map<String, String> getSubClassifications(
			List<String> classifications) {
		// TODO Auto-generated method stub
		Map<String, String> classificationsMap = new HashMap();
		for (String classification : classifications) {
			// classificationsMap.addAll( Arrays.asList(
			// classification.split("\\+")));
			for (String subClassification : Arrays.asList(classification
					.split("\\+"))) { // use + to combine classes -> hidden
										// bonus feature ;)
				classificationsMap.put(subClassification, classification);
			}
		}
		return classificationsMap;
	}

	/**
	 * Calculates Land Use Mix
	 * 
	 * @param areas
	 *            Collection of area values for each classification
	 * @param totalArea
	 *            The total area covered by all the land use classifications
	 * @return The Land Use Mix Measure
	 */
	private static Double calculateLUM(Collection<Double> areas,
			Double totalArea, Integer totalClasses) {
		Double landUseMixMeasure = 0.0;
		if (areas.size() == 1) {
			landUseMixMeasure = 0.0;
		} else {
			for (Double area : areas) {
				Double proportion = area / totalArea;
				// LOGGER.debug("Class Area: {} Total Area: {}", area,
				// totalArea);
				landUseMixMeasure += (((proportion) * (Math.log(proportion))) / (Math
						.log(totalClasses)));// .log(areas.size())));
				// LOGGER.debug("Areas: " + areas.size());
				// LOGGER.debug("Classes:" + totalClasses);
			}
			landUseMixMeasure = -1 * landUseMixMeasure;
		}
		return landUseMixMeasure;
	}

	private static SimpleFeatureCollection featuresInRegion(
			SimpleFeatureSource featureSource, Geometry roi) throws IOException {
		// Construct a filter which first filters within the bbox of roi and
		// then
		// filters with intersections of roi
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		FeatureType schema = featureSource.getSchema();
		// LOGGER.debug("FEATURE SOURCE SCHEMA: ", schema.toString());
		String geometryPropertyName = schema.getGeometryDescriptor()
				.getLocalName();

		Filter filter = ff.intersects(ff.property(geometryPropertyName),
				ff.literal(roi));

		// collection of filtered features
		return featureSource.getFeatures(filter);
	}

	private static SimpleFeatureCollection trimFeaturesToRegion(
			SimpleFeatureCollection features, Geometry roi) {
		List<SimpleFeature> trimmedFeatures = new ArrayList<SimpleFeature>();

		SimpleFeatureIterator iter = features.features();
		try {
			while (iter.hasNext()) {
				SimpleFeature feature = iter.next();
				Geometry parcelGeom = ((Geometry) feature.getDefaultGeometry()).buffer(0);
			
				Geometry trimmedGeom = parcelGeom.intersection(roi.buffer(0));

				for (int i = 0; i < trimmedGeom.getNumGeometries(); i++) {
				//	LOGGER.debug("Geom: " + trimmedGeom.getGeometryN(i).toText());
					trimmedFeatures.add(buildFeature(feature,
							trimmedGeom.getGeometryN(i), i));
				}
			}
//		} catch (TopologyException t) {
//			if (roi.isValid()) {
//				LOGGER.debug("ROI Geom valid");
//			} else {
//				LOGGER.debug("ROI Geom invalid");
//			}
//			LOGGER.debug(roi.toString());
		} finally {
			iter.close();
		}
		// GeoJSONUtilities.writeFeatures(DataUtilities.collection(trimmedFeatures),
		// new File("test_output/trimmedFeatures" + roi.hashCode() + ".json"));
		// allClippedParcels.addAll(trimmedFeatures);
		return DataUtilities.collection(trimmedFeatures);

	}

	private static SimpleFeature buildFeature(SimpleFeature feature,
			Geometry trimmedGeom, int subID) {

		SimpleFeatureType featureType = buildPolygonFeatureType(feature
				.getFeatureType());

		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);

		// Add everything the geometry
		for (AttributeDescriptor attrName : featureType
				.getAttributeDescriptors()) {
			if (attrName.getLocalName() != featureType.getGeometryDescriptor()
					.getLocalName()) {
				builder.set(attrName.getLocalName(),
						feature.getAttribute(attrName.getLocalName()));
			}
		}

		// Add the geometry
		builder.set(feature.getDefaultGeometryProperty().getName(), trimmedGeom);

		return builder.buildFeature(feature.getID() + "." + subID);
	}

	private static SimpleFeatureType buildPolygonFeatureType(
			SimpleFeatureType featureType) {
		SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();

		// set the name
		b.setName("Parcel");

		// add some properties
		b.addAll(featureType.getAttributeDescriptors());
		b.remove(featureType.getGeometryDescriptor().getLocalName());
		// add a geometry property
		b.setCRS(featureType.getCoordinateReferenceSystem()); // set crs first
		b.add(featureType.getGeometryDescriptor().getLocalName(), Polygon.class); // then
																					// add
																					// geometry
		// LOGGER.debug(featureType.getGeometryDescriptor().getLocalName());
		b.setDefaultGeometry(featureType.getGeometryDescriptor().getLocalName());
		return b.buildFeatureType();
	}
}
