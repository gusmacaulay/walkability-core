/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import java.util.ArrayList;
//import java.util.concurrent.ForkJoinPool;
//import java.util.concurrent.RecursiveAction;
import jsr166y.ForkJoinPool;
import jsr166y.RecursiveAction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates connectivity for a set of regions using a Fork/Join for concurrency
 * @author amacaulay
 */
public class ConnectivityIndexFJ extends RecursiveAction {

	private static final long serialVersionUID = 1L;
	static final Logger LOGGER  = LoggerFactory.getLogger(ConnectivityIndexFJ.class);
    private transient SimpleFeatureCollection results;
    private final transient SimpleFeatureSource roadsFeatureSource;
    private final transient SimpleFeatureCollection regions;

    public SimpleFeatureCollection getResults() {
        return results;
    }
    
    /**
     * @param The road network to count connections from
     * @param regions The regions of interest to calculate connectivity in
     */
    public ConnectivityIndexFJ(SimpleFeatureSource roadsFeatureSource, SimpleFeatureCollection regionsFeatureCollection) {
        this.roadsFeatureSource = roadsFeatureSource;
        this.regions = regionsFeatureCollection;
        this.results = FeatureCollections.newCollection();
    }

    /**
     * Computes the connectivity index, for a single region, otherwise if there are
     * multiple regions it splits the task into single region operations and invokes separately
     * note: this is slightly different from normal fork/join which bisects until the threshold condition is met.
     */
    @Override
    protected void compute() {
        if (regions.size() == 1) { //if there is only one region compute the connectivity and store in results
            try {
                SimpleFeature connectivityFeature = ConnectivityIndex.connectivity(roadsFeatureSource, regions.features().next());
                results.add(connectivityFeature);
            } catch (Exception e) {
                this.completeExceptionally(e);
            }
        } else { //otherwise split the regions into single region arrays and invokeAll
            ArrayList<ConnectivityIndexFJ> indexers = new ArrayList();
           // for (int i = 0; i < regions.length; i++) {
            FeatureIterator features = regions.features();
            while(features.hasNext()) {
                SimpleFeatureCollection singleRegionCollection = FeatureCollections.newCollection();
                SimpleFeature feature = (SimpleFeature)features.next();
                singleRegionCollection.add(feature);
                ConnectivityIndexFJ cifj = new ConnectivityIndexFJ(roadsFeatureSource, singleRegionCollection);
                indexers.add(cifj);
            }
            invokeAll(indexers);
            for (ConnectivityIndexFJ cifj : indexers) {
//                    System.out.println("Appending result: " + String.valueOf(cifj.results[0]));
                if (cifj.isCompletedAbnormally()) {
                    this.completeExceptionally(cifj.getException());
                }
                results.addAll(cifj.results);
               
            }
        }
    }

    /**
     * Sets up the ForkJoinPool and then calls invoke to calculate connectivity for all regions available
     */
    public void connectivity() {
        //Get the available processors, processors==threads is probably best?
        Runtime runtime = Runtime.getRuntime();
        int nProcessors = runtime.availableProcessors();
        int nThreads = nProcessors;

        LOGGER.debug("Initialising ForkJoinPool with {}", nThreads);
        //Fork/Join handles threads for me, all I do is invoke
        ForkJoinPool fjPool = new ForkJoinPool(nThreads);
        fjPool.invoke(this);
        if (this.isCompletedAbnormally()) {
            LOGGER.error("ForkJoin connectivity calculation failed: {}", this.getException().toString());
            this.completeExceptionally(this.getException());
        } 
    }
}