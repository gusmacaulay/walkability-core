/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import org.geotools.data.FileDataStore;
import org.opengis.feature.Feature;

/**
 *
 * @author amacaulay
 */
public class ConnectivityIndexFJ extends RecursiveAction {

    public double[] results;
    private final FileDataStore roadsDataStore;
    private final Feature[] regions;

    /**
     * 
     * @param roadsDataStore
     * @param regions
     */
    public ConnectivityIndexFJ(FileDataStore roadsDataStore, Feature[] regions) {
        this.roadsDataStore = roadsDataStore;
        this.regions = regions.clone();
        this.results = new double[regions.length];
    }

    /**
     * 
     */
    @Override
    protected void compute() {
        if (regions.length == 1) {
            try {
                Geometry geom = (Geometry) regions[0].getDefaultGeometryProperty().getValue();
                double connectivity = ConnectivityIndex.connectivity(roadsDataStore.getFeatureSource(), geom);
//              System.out.println("Connectivity: " + String.valueOf(connectivity));
                results[0] = connectivity;
            } catch (IOException e) {
                this.completeExceptionally(e);
            }
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
                if (cifj.isCompletedAbnormally()) {
                    this.completeExceptionally(cifj.getException());
                }
                this.results[i] = cifj.results[0];
                i++;
            }
        }
    }

    /**
     * 
     */
    public void connectivity() {
        //Get the available processors, processors==threads is probably best?
        Runtime runtime = Runtime.getRuntime();
        int nrOfProcessors = runtime.availableProcessors();
        int nThreads = nrOfProcessors;

        //Fork/Join handles threads for me, all I do is invoke
        ForkJoinPool fjPool = new ForkJoinPool(nThreads);
        fjPool.invoke(this);
        if (this.isCompletedAbnormally()) {
            this.completeExceptionally(this.getException());
        } 
    }
}