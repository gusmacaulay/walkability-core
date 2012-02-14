/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools;


import java.awt.Dimension;
import java.io.File;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.gce.geotiff.GeoTiffWriter;

import org.geotools.process.raster.VectorToRasterProcess;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.geometry.Envelope;
import org.geotools.util.DefaultProgressListener;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 *
 * @author gus
 */
public class ConnectivityIndex {

    public static void main(String[] args) throws Exception {
        //Read a shapefile in ...
        SimpleFeatureSource featureSource = openShapeFile("/home/gus/SRC/geoserver-trunk/data/release/data/taz_shapes/tasmania_roads.shp");
        //Do something to it ...
        SimpleFeatureCollection featureCollection = addValues(featureSource);
        //Write out features as coverage ...
        GridCoverage coverage = collectionToCoverage(featureCollection, "Value");
        //Write coverage to file ...
        File geotiff = writeCoverage("/home/gus/coverage.geotiff",coverage);
    }

    private static SimpleFeatureSource openShapeFile(String filename) throws Exception {
        File shapeFile = new File(filename);
        // Connect to the shapefile
        FileDataStore dataStore = FileDataStoreFinder.getDataStore(shapeFile);
        return dataStore.getFeatureSource();
    }

    private static SimpleFeatureCollection addValues(SimpleFeatureSource featureSource) throws Exception {
        
        
        SimpleFeatureType sft = featureSource.getSchema();
        
        //Create the new type using the former as a template
        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
        stb.init(sft);
        stb.setName("newFeatureType");

        //Add the new attribute
        stb.add("Value", Integer.class);
        SimpleFeatureType newFeatureType = stb.buildFeatureType(); 
        
        //Replace features with new features
        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFeatureType);
        SimpleFeatureCollection collection = FeatureCollections.newCollection(); 
        SimpleFeatureIterator it = featureSource.getFeatures().features();
        try {
            while (it.hasNext() && collection.size() <= 10) {
                SimpleFeature sf = it.next();
                sfb.addAll(sf.getAttributes());
                sfb.add(Integer.valueOf(1));
                collection.add(sfb.buildFeature(null));
            }
        } finally {
            it.close();
        } 
        return collection;
    }
    
    private static GridCoverage collectionToCoverage(SimpleFeatureCollection featureCollection, Object attribute) throws Exception {
     
        Dimension gridDim = new Dimension();
        gridDim.setSize(1000,1000);
        Envelope bounds = featureCollection.getBounds();
        String covName = "CoverageTest";
        DefaultProgressListener p = new DefaultProgressListener();
        
        GridCoverage coverage =
            VectorToRasterProcess.process(featureCollection,attribute,gridDim,bounds,covName,p); 

        return coverage;
        
    }
    
    private static File writeCoverage(String fileName, GridCoverage coverage) throws Exception {
        File coverageFile = new File(fileName);
        
        GeoTiffWriter gtw = new GeoTiffWriter(coverageFile);
        gtw.write(coverage,null);
       
        return coverageFile;
    }
}
