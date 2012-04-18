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
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;

/**
 *
 * @author amacaulay
 */
public class GeoJSONUtilities {
      public static void writeFeatures(SimpleFeatureCollection features, File file) {
        FeatureJSON fjson = new FeatureJSON();
        try {
            OutputStream os = new FileOutputStream(file);
            fjson.writeFeatureCollection(features,os);
            fjson.writeCRS(features.getSchema().getCoordinateReferenceSystem(), os);
        } catch (IOException e) {
            System.out.println("Failed to write feature collection" + e.getMessage());
        }
    }
}
