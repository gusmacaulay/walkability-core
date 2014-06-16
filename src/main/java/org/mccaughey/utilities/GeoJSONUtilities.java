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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import au.org.aurin.data.store.client.DataStoreClient;
//import au.org.aurin.data.store.client.DataStoreClientImpl;
//import au.org.aurin.security.util.SslUtil;

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
   * Writes out a SimpleFeatureCollection to a file as geojson
   * 
   * @param features
   *          The features to write
   * @param file
   *          The file to write to
   */
  public static void writeFeatures(SimpleFeatureCollection features, File file) {
    FeatureJSON fjson = new FeatureJSON();
    OutputStream os;
    try {
      os = new FileOutputStream(file);
      try {
        // fjson.writeCRS(features.getSchema().getCoordinateReferenceSystem(),
        // os);

        if (features.getSchema().getCoordinateReferenceSystem() != null) {
          fjson.setEncodeFeatureCollectionBounds(true);
          fjson.setEncodeFeatureCollectionCRS(true);
        } else {
          LOGGER.debug("CRS is null");
        }
        // LOGGER.debug("CRS: {}", features.getSchema()
        // .getCoordinateReferenceSystem().toString());
        // if
        // (features.getSchema().getCoordinateReferenceSystem().toString().contains("UNIT[\"m"))
        // {
        // LOGGER.debug("CRS in metres!");
        // } else {
        // LOGGER.error("CRS not in metres");
        // }
        fjson.writeFeatureCollection(features, os);
      } finally {
        os.close();
      }
    } catch (FileNotFoundException e1) {
      LOGGER.error("Failed to write feature collection " + e1.getMessage());
    } catch (IOException e) {
      LOGGER.error("Failed to write feature collection " + e.getMessage());
    }
  }

  /**
   * Writes a SimpleFeatureCollection to a URL as geojson
   * 
   * @param features
   *          The features to write out
   * @param file
   *          The URL to write to (will overwrite existing)
   */
  public static URL writeFeatures(SimpleFeatureCollection features, URL dataStoreURL) {
    String dataStore = dataStoreURL.toString();

    try {
      LOGGER.debug("Writing to File resource {}", dataStore);
      writeFeatures(features, new File(dataStoreURL.toURI()));
      return dataStoreURL;
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return null; // FIXME: add proper error handling
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
      try {
        // fjson.setEncodeFeatureCRS(true);
        // fjson.writeCRS(feature.getType().getCoordinateReferenceSystem(), os);
        fjson.writeFeature(feature, os);
      } finally {
        os.close();
      }
    } catch (IOException e) {
      LOGGER.error("Failed to write feature collection" + e.getMessage());
    }
  }

  /**
   * Reads a single feature from GeoJSON
   * 
   * @param url
   *          A URL pointing to a GeoJSON feature
   * @return The feature from the URL
   * @throws IOException
   */
  public static SimpleFeature readFeature(URL url) throws IOException {
    FeatureJSON io = new FeatureJSON();
    // io.setEncodeFeatureCRS(true);
    return io.readFeature(url.openConnection().getInputStream());
  }

  /**
   * Gets a FeatureIterator from a GeoJSON URL, does not need to read all the features?
   * 
   * @param url
   *          The FeatureCollection URL
   * @return An Iterator for the features at the URL
   * @throws IOException
   */
  public static FeatureIterator<SimpleFeature> getFeatureIterator(URL url) throws IOException {
    LOGGER.debug("Reading features from URL {}", url);
    FeatureJSON io = new FeatureJSON();
    // SslUtil.trustSelfSignedSSL();
    io.setEncodeFeatureCollectionCRS(true);
    return io.streamFeatureCollection(url.openConnection().getInputStream());
  }

  /**
   * Gets a SimpleFeatureCollection from a GeoJSON URL - reads all the features
   * 
   * @param url
   *          The FeatureCollection URL
   * @return The features at the URL
   * @throws IOException
   */
  public static SimpleFeatureCollection readFeatures(URL url) throws IOException {
    FeatureJSON io = new FeatureJSON();
    // FeatureJSON crsReader = new FeatureJSON();

    io.setEncodeFeatureCRS(true);
    // CoordinateReferenceSystem crs = crsReader.readCRS(url.openConnection().getInputStream());
    // io.setEncodeFeatureCollectionCRS(true);

    LOGGER.debug("READING GeoJSON from {}", url);
    // io.readCRS(url.openConnection().getInputStream()));
    FeatureIterator<SimpleFeature> features = null;
    try {
      if (url.toString().contains(".geojson.gz")) {
        CompressorInputStream input;

        input = new CompressorStreamFactory().createCompressorInputStream(new BufferedInputStream(new FileInputStream(
           new File(url.toURI()))));
        features = io.streamFeatureCollection(input);

      } else {
        features = io.streamFeatureCollection(url.openConnection().getInputStream());
      }
    } catch (CompressorException e) {
      throw new IOException(e);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DefaultFeatureCollection collection = new DefaultFeatureCollection();

    while (features.hasNext()) {
      SimpleFeature feature = features.next();
      collection.add(feature);
    }

    return collection;
  }
}
