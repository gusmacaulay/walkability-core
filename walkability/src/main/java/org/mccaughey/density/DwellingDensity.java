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

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.util.Collection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
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
public final class DwellingDensity {
    
    static final Logger LOGGER = LoggerFactory.getLogger(DwellingDensity.class);
    
    private DwellingDensity() {
    }
    
    public static SimpleFeature averageDensity(SimpleFeatureSource dwellingSource, SimpleFeature roi, String densityAttribute) {
        try {
            SimpleFeatureIterator dwellings = featuresInRegion(dwellingSource, (Geometry) roi.getDefaultGeometry()).features();
            Double totalDensity = 0.0;
            
            while(dwellings.hasNext()) {
                SimpleFeature dwelling = dwellings.next();
                Double count = (Double)dwelling.getAttribute(densityAttribute);
                totalDensity += count/((Geometry)dwelling.getDefaultGeometry()).getArea();
                
            }
            
            return buildFeature(roi,totalDensity);
        } catch (IOException ioe) {
            LOGGER.error("Error selecting features in region");
            return null;
        }
        
    }
    
    private static SimpleFeature buildFeature(SimpleFeature region, Double density) {
         
            SimpleFeatureType sft = (SimpleFeatureType) region.getType();
            SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
            stb.init(sft);
            stb.setName("densityFeatureType");
            stb.add("AverageDensity", Double.class);
            SimpleFeatureType landUseMixFeatureType = stb.buildFeatureType();
            SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(landUseMixFeatureType);
            sfb.addAll(region.getAttributes());
            sfb.add(density);
            return sfb.buildFeature(region.getID());
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
