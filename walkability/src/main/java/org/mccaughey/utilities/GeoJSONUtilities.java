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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;

import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.org.aurin.data.store.client.DataStoreClient;
import au.org.aurin.data.store.client.DataStoreClientImpl;
import au.org.aurin.security.util.SslUtil;

/**
 * Handles reading and writing of GeoJSON in and out of Geotools
 * 
 * @author amacaulay
 */
public final class GeoJSONUtilities {

	static final Logger LOGGER = LoggerFactory.getLogger(GeoJSONUtilities.class);

	private GeoJSONUtilities() {
	}

	private static void writeFeatures(SimpleFeatureCollection features, File file) {
		FeatureJSON fjson = new FeatureJSON();
		try {
			OutputStream os = new FileOutputStream(file);
			// fjson.writeCRS(features.getSchema().getCoordinateReferenceSystem(),
			// os);

//			if (features.getSchema().getCoordinateReferenceSystem() != null) {
//				fjson.setEncodeFeatureCollectionBounds(true);
//				fjson.setEncodeFeatureCollectionCRS(true);
//			} else {
//				throw new IOException("CRS is null");
//			}
//			LOGGER.info("CRS: {}", features.getSchema().getCoordinateReferenceSystem().toString());
//			if (features.getSchema().getCoordinateReferenceSystem().toString().contains("UNIT[\"m")) {
//				LOGGER.info("CRS in metres!");
//			} else {
//				LOGGER.error("CRS not in metres");
//			}
			
			fjson.writeFeatureCollection(features, os);
			os.close();
		} catch (IOException e) {
			LOGGER.error("Failed to write feature collection " + e.getMessage());
		}
	}

	/**
	 * Writes a SimpleFeatureCollection to a URL as geojson
	 * 
	 * @param features
	 *            The features to write out
	 * @param file
	 *            The URL to write to (will overwrite existing)
	 */
	public static URL writeFeatures(SimpleFeatureCollection features, URL dataStoreURL) {
		SslUtil.trustSelfSignedSSL();
		DataStoreClient store = new DataStoreClientImpl();
		String dataStore = dataStoreURL.toString();

		URL featuresURL = null;
		try {
			// LOGGER.info("DATA STORE: {}", dataStore);
			if (dataStore.toLowerCase().startsWith("file:/")) {
				LOGGER.info("Writing to File resource {}", dataStore);
				featuresURL = dataStoreURL;
				writeFeatures(features, new File(dataStoreURL.toURI()));
				return dataStoreURL;
			} else {
				store.setUrl(dataStore);

				featuresURL = new URL(store.createStorageLocation());
				LOGGER.info("New geojson resource {}", featuresURL);

				FeatureJSON fjson = new FeatureJSON();

				ByteArrayOutputStream sos = new ByteArrayOutputStream();

				fjson.writeFeatureCollection(features, sos);
				String urlString = featuresURL.toString();
				store.storeGeoJsonData(urlString, sos.toString());
			}
			return featuresURL;
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
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
			fjson.setEncodeFeatureCRS(true);
			fjson.writeCRS(feature.getType().getCoordinateReferenceSystem(), os);
			fjson.writeFeature(feature, os);
		} catch (IOException e) {
			LOGGER.error("Failed to write feature collection" + e.getMessage());
		}
	}

	/**
	 * Reads a single feature from GeoJSON
	 * 
	 * @param url
	 *            A URL pointing to a GeoJSON feature
	 * @return The feature from the URL
	 * @throws IOException
	 */
	public static SimpleFeature readFeature(URL url) throws IOException {
		FeatureJSON io = new FeatureJSON();
		// io.setEncodeFeatureCRS(true);
		return io.readFeature(url.openConnection().getInputStream());
	}

	/**
	 * Gets a FeatureIterator from a GeoJSON URL, does not need to read all the
	 * features?
	 * 
	 * @param url
	 *            The FeatureCollection URL
	 * @return An Iterator for the features at the URL
	 * @throws IOException
	 */
	public static FeatureIterator<SimpleFeature> getFeatureIterator(URL url) throws IOException {
		LOGGER.info("Reading features from URL {}", url);
		FeatureJSON io = new FeatureJSON();
		SslUtil.trustSelfSignedSSL();
		io.setEncodeFeatureCollectionCRS(true);
		return io.streamFeatureCollection(url.openConnection().getInputStream());
	}

	/**
	 * Gets a SimpleFeatureCollection from a GeoJSON URL - reads all the
	 * features
	 * 
	 * @param url
	 *            The FeatureCollection URL
	 * @return The features at the URL
	 * @throws IOException
	 */
	public static SimpleFeatureCollection readFeatures(URL url) throws IOException {
		FeatureJSON io = new FeatureJSON();
		io.setEncodeFeatureCollectionCRS(true);

		LOGGER.info("READING GeoJSON from {}", url);
		// io.readCRS(url.openConnection().getInputStream()));
		FeatureIterator<SimpleFeature> features = io.streamFeatureCollection(url.openConnection().getInputStream());
		SimpleFeatureCollection collection = FeatureCollections.newCollection();

		while (features.hasNext()) {
			SimpleFeature feature = (SimpleFeature) features.next();
			collection.add(feature);
		}

		return collection;
	}
}
