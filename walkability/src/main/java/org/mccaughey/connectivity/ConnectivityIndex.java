/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;


import java.io.File;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.coverage.grid.GridCoverage;

/**
 *
 * @author gus
 */
public class ConnectivityIndex {

    public static void main(String[] args) throws Exception {
        //Read a shapefile in ...
        SimpleFeatureSource featureSource = openShapeFile("/home/gus/SRC/geoserver-trunk/data/release/data/taz_shapes/tasmania_roads.shp");

     
    }

    private static SimpleFeatureSource openShapeFile(String filename) throws Exception {
        File shapeFile = new File(filename);
        // Connect to the shapefile
        FileDataStore dataStore = FileDataStoreFinder.getDataStore(shapeFile);
        return dataStore.getFeatureSource();
    }
}
