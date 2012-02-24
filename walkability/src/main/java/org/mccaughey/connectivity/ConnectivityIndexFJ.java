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
 * Calculates connectivity for a set of regions using a Fork/Join for concurrency
 * @author amacaulay
 */
public class ConnectivityIndexFJ extends RecursiveAction {

    public double[] results;
    private final FileDataStore roadsDataStore;
    private final Feature[] regions;

    /**
     * 
     * @param The road network to count connections from
     * @param regions The regions of interest to calculate connectivity in
     */
    public ConnectivityIndexFJ(FileDataStore roadsDataStore, Feature[] regions) {
        this.roadsDataStore = roadsDataStore;
        this.regions = regions.clone();
        this.results = new double[regions.length];
    }

    /**
     * Computes the connectivity index, for a single region, otherwise if there are
     * multiple regions it splits the task into single region operations and invokes separately
     * note: this is slightly different from normal fork/join which bisects until the threshold condition is met.
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
     * Sets up the ForkJoinPool and then calls invoke to calculate connectivity for all regions available
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