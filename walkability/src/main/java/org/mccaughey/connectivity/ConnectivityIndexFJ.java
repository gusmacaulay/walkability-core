/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import com.vividsolutions.jts.geom.Geometry;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import org.apache.commons.lang3.ArrayUtils;
import org.geotools.data.FileDataStore;

import org.opengis.feature.Feature;

/**
 *
 * @author amacaulay
 */
public class ConnectivityIndexFJ extends RecursiveAction {

    private final double[] results;
    private final FileDataStore roadsDataStore;
    private final Feature[] regions;

    public ConnectivityIndexFJ(FileDataStore roadsDataStore, Feature[] regions) {
        this.roadsDataStore = roadsDataStore;
        this.regions = regions;
        this.results = new double[regions.length];
    }

    @Override
    protected void compute() {

        // Connect to the network shapefile and regions shapefile, process with ConnectivityIndex
        try {

            if (regions.length == 1) {
                Geometry geom = (Geometry) regions[0].getDefaultGeometryProperty().getValue();
                double connectivity = ConnectivityIndex.connectivity(roadsDataStore.getFeatureSource(), geom);
//              System.out.println("Connectivity: " + String.valueOf(connectivity));
                results[0] = connectivity;
            } else {
                ArrayList<ConnectivityIndexFJ> indexers = new ArrayList();
                for (int i = 0; i < regions.length; i++) {
                    Feature[] singleRegion = new Feature[1];
                    singleRegion[0] = regions[i];
                    ConnectivityIndexFJ cifj = new ConnectivityIndexFJ(roadsDataStore, singleRegion);
                    indexers.add(cifj);
                }
                invokeAll(indexers);
                int i = 0;
                for (ConnectivityIndexFJ cifj : indexers) {
//                    System.out.println("Appending result: " + String.valueOf(cifj.results[0]));
                    this.results[i] = cifj.results[0];
                    i++;
                }
            }
            // assert (connectivity == 27.443516115141286); //correct number for tasmania_roads.shp
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public void connectivity() throws Exception {
        //Get the available processors, processors==threads is probably best?
        Runtime runtime = Runtime.getRuntime();
        int nrOfProcessors = runtime.availableProcessors();
        int nThreads = nrOfProcessors;
        
        
        ForkJoinPool fjPool = new ForkJoinPool(nThreads);
        fjPool.invoke(this);
    }
}