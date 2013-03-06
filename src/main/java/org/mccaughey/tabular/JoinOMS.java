package org.mccaughey.tabular;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Name;
import oms3.annotations.Out;

import org.geotools.data.DataStore;
import org.geotools.data.FileDataStoreFactorySpi;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Performs a join on two tables - this should be refactored so it can handle
 * spatial and non-spatial datasets NOTE: THIS CLASS IS A HACK while we wait for
 * AURIN to implement a real join, I reccomend you DON'T use it.
 * 
 * @author amacaulay
 * 
 *
 */
@Name("join")
public class JoinOMS {
  private static final int FEATURE_STORE_THRESHOLD = 10000;

  static final Logger LOGGER = LoggerFactory.getLogger(JoinOMS.class);

  @In
  @Name("Non-spatial Table")
  @Description("Tabular data with no geometry attribute")
  SimpleFeatureSource nonSpatialTable;

  @In
  @Name("Spatial Table")
  @Description("Data Source including geometry attribute")
  SimpleFeatureSource spatialTable;

  @In
  @Name("Join Attribute")
  @Description("The common attribute between tables")
  String joinColumn;

  @In
  URL dataStore;

  @Out
  SimpleFeatureSource result;

  /**
   * Performs a join of a CSV with Geotools SimpleFeatureSource
   */
  @Execute
  public void join() {
    result = null;
  }

  private void joinDataSets(Map<String, List<String>> lookupTable,
      SimpleFeatureType newFeatureType, DataStore myData) throws IOException {
    SimpleFeatureIterator features = spatialTable.getFeatures().features();
    try {
      // SimpleFeature[] newFeatures = new
      // SimpleFeature[spatialTable.getFeatures().size()];
      int i = 0;
      SimpleFeatureStore featureStore = (SimpleFeatureStore) myData
          .getFeatureSource(newFeatureType.getName());//
      SimpleFeatureCollection collection = FeatureCollections
          .newCollection("internal");
      while (features.hasNext()) {
        SimpleFeature feature = features.next();
        List joinValues = lookupTable.get(feature.getAttribute(joinColumn)
            .toString());
        if (joinValues == null) {
          LOGGER.warn("Missing Classification for key: {}", feature
              .getAttribute(joinColumn).toString());
        } else {
          SimpleFeature joinFeature = buildFeature(feature, newFeatureType,
              joinValues);
          collection.add(joinFeature);

          i++;
          if (i > FEATURE_STORE_THRESHOLD) {
            featureStore.addFeatures(collection);
            collection = FeatureCollections.newCollection("internal");
            i = 0;
          }
        }
      }
      // }
      featureStore.addFeatures(collection);
      result = featureStore;// DataUtilities.collection(newFeatures));
    } finally {
      features.close();
    }
  }

  private static SimpleFeatureType createNewFeatureType(
      SimpleFeatureType baseFeatureType, List<String> newAttributeNames) {
    // SimpleFeatureType baseFeatureType = (SimpleFeatureType)
    // baseFeature.getType();
    SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
    stb.init(baseFeatureType);
    stb.setName("newFeatureType");
    // Add new attributes to feature type
    for (String attr : newAttributeNames) {
      stb.add(attr, String.class);
    }
    return stb.buildFeatureType();
  }

  private static SimpleFeature buildFeature(SimpleFeature baseFeature,
      SimpleFeatureType newFT, List<String> newValues) {
    SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFT);
    sfb.addAll(baseFeature.getAttributes());
    for (String value : newValues) {
      sfb.add(value);
    }
    return sfb.buildFeature(baseFeature.getID());

  }
}
