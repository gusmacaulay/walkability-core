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

import au.edu.unimelb.eresearch.spring.http.converter.geojson.FeatureHttpMessageConverter;
import au.org.aurin.gis.client.SrsHandlerClient;
import au.org.aurin.gis.client.SrsHandlerClient;
import au.org.aurin.security.util.SslUtil;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

/**
 * Handles reading and writing of GeoJSON in and out of Geotools
 *
 * @author amacaulay
 */
public final class GeoJSONUtilities {

    static final Logger LOGGER = LoggerFactory.getLogger(GeoJSONUtilities.class);

    private GeoJSONUtilities() {
    }

    /**
     * Writes a SimpleFeatureCollection to file
     *
     * @param features The features to write out
     * @param file The file to write to (will overwrite existing)
     */
    public static void writeFeatures(SimpleFeatureCollection features, File file) {
        FeatureJSON fjson = new FeatureJSON();
        try {
            OutputStream os = new FileOutputStream(file);
            // fjson.writeCRS(features.getSchema().getCoordinateReferenceSystem(), os);
            
            LOGGER.info("CRS: {}",features.getSchema().getCoordinateReferenceSystem().toString());
                    if (features.getSchema().getCoordinateReferenceSystem().toString().contains("UNIT[\"m")) {
                        LOGGER.info("CRS in metres!"); 
                    }
                    else
                        LOGGER.error("CRS not in metres");
            try {
                LOGGER.info("CRS ID: {} ??", CRS.lookupIdentifier(features.getSchema().getCoordinateReferenceSystem(), true));
                URL srsHandlerClientURL = new URL("https://dev-api.aurin.org.au/aurin-srs-handler/");
             //   features = washProjection(features, srsHandlerClientURL);
            } catch (Exception e) {
            }

            fjson.setEncodeFeatureCollectionBounds(true);
            fjson.setEncodeFeatureCollectionCRS(true);
            fjson.writeFeatureCollection(features, os);
        } catch (IOException e) {
            LOGGER.error("Failed to write feature collection" + e.getMessage());
        }
    }

    /**
     * Writes a single feature to file
     *
     * @param feature
     * @param file
     */
    public static void writeFeature(SimpleFeature feature, File file) {
        FeatureJSON fjson = new FeatureJSON();
        try {
            OutputStream os = new FileOutputStream(file);
            fjson.setEncodeFeatureCRS(true);
            //fjson.writeCRS(feature.getType().getCoordinateReferenceSystem(), os);
            fjson.writeFeature(feature, os);
        } catch (IOException e) {
            LOGGER.error("Failed to write feature collection" + e.getMessage());
        }
    }

    /**
     * Reads a single feature from GeoJSON
     *
     * @param url A URL pointing to a GeoJSON feature
     * @return The feature from the URL
     * @throws IOException
     */
    public static SimpleFeature readFeature(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();
        io.setEncodeFeatureCRS(true);
        return io.readFeature(url.openConnection().getInputStream());
    }

    /**
     * Gets a FeatureIterator from a GeoJSON URL, does not need to read all the
     * features?
     *
     * @param url The FeatureCollection URL
     * @return An Iterator for the features at the URL
     * @throws IOException
     */
    public static FeatureIterator<SimpleFeature> getFeatureIterator(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();
        io.setEncodeFeatureCollectionCRS(true);
        return io.streamFeatureCollection(url.openConnection().getInputStream());
    }

    /**
     * Gets a SimpleFeatureCollection from a GeoJSON URL - reads all the
     * features
     *
     * @param url The FeatureCollection URL
     * @return The features at the URL
     * @throws IOException
     */
    public static SimpleFeatureCollection readFeatures(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();
        io.setEncodeFeatureCollectionCRS(true);

        LOGGER.info("READ CRS: {}", io.readCRS(url.openConnection().getInputStream()));
        FeatureIterator<SimpleFeature> features = io.streamFeatureCollection(url.openConnection().getInputStream());
        SimpleFeatureCollection collection = FeatureCollections.newCollection();

        while (features.hasNext()) {
            SimpleFeature feature = (SimpleFeature) features.next();
            collection.add(feature);
        }

        return collection;
    }

    public static SimpleFeatureCollection washProjection(SimpleFeatureCollection features, URL projectionWasher) {
       
        SslUtil.trustSelfSignedSSL();
        SrsHandlerClient srsHandlerClient = new SrsHandlerClient();

        List<HttpMessageConverter<?>> messageConverters = new ArrayList<HttpMessageConverter<?>>();
        messageConverters.add(new StringHttpMessageConverter());

        FeatureHttpMessageConverter geoJsonMessageConverter = new FeatureHttpMessageConverter();
        geoJsonMessageConverter.setFeatureJSON(new FeatureJSON());
        messageConverters.add(geoJsonMessageConverter);

        RestTemplate nonSpringRestTemplate = new RestTemplate();
        nonSpringRestTemplate.setMessageConverters(messageConverters);

        srsHandlerClient.setRestTemplate(nonSpringRestTemplate);
        srsHandlerClient.setUrl(projectionWasher.toString());

        Feature feature = features.features().next();
        List<SimpleFeature> featureList = new ArrayList();
        featureList.add(features.features().next());
        SimpleFeatureType ft = features.getSchema();
       // DefaultFeatureCollection collection = new DefaultFeatureCollection(
                //null,ft );
       // collection.add(features.features().next());
        String epsg = srsHandlerClient.lookupGrid(features);
        LOGGER.info("Washed EPSG: {}",epsg);
//        try {
//            FeatureCollection featureCollection = collectionPolygon(121, -34, 4);
//
//            System.out.println("non-springified lookup result: "
//                    + srsHandlerClient.lookupGrid(featureCollection));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
         return features;
    }
}
