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

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.util.*;
import org.geotools.data.DataUtilities;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public class LandUseMix {

    static final Logger LOGGER = LoggerFactory.getLogger(LandUseMix.class);

    public static SimpleFeatureCollection summarise(SimpleFeatureSource landUse, FeatureIterator<SimpleFeature> regions, List<String> classifications) {
        List<SimpleFeature> lumFeatures = new ArrayList();
        while (regions.hasNext()) {
            SimpleFeature lumFeature = summarise(landUse, regions.next(), classifications);
            lumFeatures.add(lumFeature);
        }
        return DataUtilities.collection(lumFeatures);
    }

    public static SimpleFeature summarise(SimpleFeatureSource landUse, SimpleFeature region, List<String> classifications) {

        try {
            Geometry regionGeom = (Geometry) region.getDefaultGeometry();

            SimpleFeatureIterator parcels = (featuresInRegion(landUse, regionGeom)).features();
            Map classificationAreas = new HashMap();
            double totalArea = 0.0;
            while (parcels.hasNext()) {
                SimpleFeature parcel = parcels.next();
                Geometry parcelGeom = (Geometry) parcel.getDefaultGeometry();
                String classification = (String) parcel.getAttribute("CATEGORY");
                //  LOGGER.info("Classification: {}", classification);
                if (classifications.contains(classification)) {
                    //     LOGGER.info("Classification: {}", classification);
                    Double parcelArea = parcelGeom.intersection(regionGeom).getArea();
                    totalArea += parcelArea;
                    Double area = parcelArea;
                    if (classificationAreas.containsKey(classification)) {
                        area = (Double) classificationAreas.get(classification) + area;
                    }
                    classificationAreas.put(classification, area);

                }

            }

            Collection<Double> areas = classificationAreas.values();
            SimpleFeatureType sft = (SimpleFeatureType) region.getType();
            SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
            stb.init(sft);
            stb.setName("landUseMixFeatureType");
            for(String classification : classifications) {
                stb.add(classification, Double.class);
            }
            //Add the land use mix attribute
            stb.add("LandUseMixMeasure", Double.class);
            SimpleFeatureType landUseMixFeatureType = stb.buildFeatureType();
            SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(landUseMixFeatureType);
            sfb.addAll(region.getAttributes());
            for(String classification : classifications) {
                sfb.add(classificationAreas.get(classification));
            }
            Double landUseMixMeasure = calculateLUM(areas, totalArea);
            sfb.add(landUseMixMeasure);
            SimpleFeature landUseMixFeature = sfb.buildFeature(null);
            return landUseMixFeature;
            // LOGGER.info("Land Use Mix Measure: {}", landUseMixMeasure);
        } catch (IOException e) {
            LOGGER.error("Failed to select land use features in region: {}", e.getMessage());
            return null;
        }
        // return region;
    }

    private static Double calculateLUM(Collection<Double> areas, Double totalArea) {
        Double landUseMixMeasure = 0.0;
        if (areas.size() == 1) {
            landUseMixMeasure = 0.0;
        } else {
            for (Double area : areas) {
                Double proportion = area / totalArea;
                //  LOGGER.info("Class Area: {} Total Area: {}", area, totalArea);
                landUseMixMeasure += (((proportion) * (Math.log(proportion))) / (Math.log(areas.size())));
            }
            landUseMixMeasure = -1 * landUseMixMeasure;
        }
        return landUseMixMeasure;
    }

    private static SimpleFeatureCollection featuresInRegion(SimpleFeatureSource featureSource, Geometry roi) throws IOException {
        //Construct a filter which first filters within the bbox of roi and then filters with intersections of roi
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        FeatureType schema = featureSource.getSchema();

        String geometryPropertyName = schema.getGeometryDescriptor().getLocalName();

        Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(roi));

        // collection of filtered features
        return featureSource.getFeatures(filter);
    }
}
