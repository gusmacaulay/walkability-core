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
package org.mccaughey.utilities;

import java.io.*;
import java.net.URL;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public class GeoJSONUtilities {

    static final Logger LOGGER = LoggerFactory.getLogger(GeoJSONUtilities.class);

    public static void writeFeatures(SimpleFeatureCollection features, File file) {
        FeatureJSON fjson = new FeatureJSON();
        try {
            OutputStream os = new FileOutputStream(file);
            fjson.writeFeatureCollection(features, os);
            fjson.writeCRS(features.getSchema().getCoordinateReferenceSystem(), os);
        } catch (IOException e) {
            System.out.println("Failed to write feature collection" + e.getMessage());
        }
    }

    public static SimpleFeature readFeature(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();
        return io.readFeature(url.openConnection().getInputStream());
    }

    public static SimpleFeatureIterator getFeatureIterator(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();
        return (SimpleFeatureIterator)io.streamFeatureCollection(url.openConnection().getInputStream());
    }
    
    public static SimpleFeatureCollection readFeatures(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();

        FeatureIterator<SimpleFeature> features = io.streamFeatureCollection(url.openConnection().getInputStream());

        SimpleFeatureCollection collection = FeatureCollections.newCollection();
        SimpleFeature feature;

        while (features.hasNext()) {
            LOGGER.info("Found");
            feature = (SimpleFeature) features.next();
            collection.add(feature);
        }

        return collection;
    }
}
