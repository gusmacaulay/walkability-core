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
package org.mccaughey.connectivity;

import java.util.ArrayList;
import java.util.Map;
import jsr166y.ForkJoinPool;
import jsr166y.RecursiveAction;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amacaulay
 */
public class NetworkBufferBatch extends RecursiveAction {

    static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferBatch.class);
    SimpleFeatureSource network;
    SimpleFeatureCollection points;
    SimpleFeatureCollection buffers;
    SimpleFeatureCollection graphs;
    Double distance;
    Double bufferSize;

    public NetworkBufferBatch(SimpleFeatureSource network, SimpleFeatureCollection points, Double distance, Double bufferSize) {
        this.network = network;
        this.points = points;
        this.distance = distance;
        this.bufferSize = bufferSize;
        this.buffers = FeatureCollections.newCollection();
        this.graphs = FeatureCollections.newCollection();

    }

    public SimpleFeatureCollection getGraphs() {
        return graphs;
    }
    public SimpleFeatureCollection createBuffers() {
        Runtime runtime = Runtime.getRuntime();
        int nProcessors = runtime.availableProcessors();
        int nThreads = nProcessors;

        LOGGER.debug("Initialising ForkJoinPool with {}", nThreads);
        //Fork/Join handles threads for me, all I do is invoke
        ForkJoinPool fjPool = new ForkJoinPool(nThreads);
        fjPool.invoke(this);

        return buffers;
    }

    @Override
    protected void compute() {
        if (points.size() == 1) { //if there is only one point compute the network region and store in buffers
            try {
                Map serviceArea = NetworkBuffer.findServiceArea(network, points.features().next(), distance, bufferSize);
                if (serviceArea != null) {
                    //List<SimpleFeature> networkBuffer = NetworkBuffer.createLinesFromEdges(serviceArea);
                    SimpleFeatureCollection graph = DataUtilities.collection(NetworkBuffer.createLinesFromEdges(serviceArea));
                    SimpleFeature networkBuffer = NetworkBuffer.createBufferFromEdges(serviceArea, bufferSize, points.getSchema().getCoordinateReferenceSystem());
                    if (networkBuffer != null) {
                        buffers.add(networkBuffer); //.addAll(DataUtilities.collection(networkBuffer));
                        graphs.addAll(graph);
                    }
                }
            } catch (Exception e) {
                this.completeExceptionally(e);
            }
        } else { //otherwise split the points into single point arrays and invokeAll
            ArrayList<NetworkBufferBatch> buffernators = new ArrayList();
            FeatureIterator features = points.features();
            while (features.hasNext()) {
                SimpleFeatureCollection singlePointCollection = FeatureCollections.newCollection();
                SimpleFeature feature = (SimpleFeature) features.next();
                singlePointCollection.add(feature);
                NetworkBufferBatch nbb = new NetworkBufferBatch(network, singlePointCollection,distance,bufferSize);
                buffernators.add(nbb);
            }
            invokeAll(buffernators);
            for (NetworkBufferBatch nbb : buffernators) {
               // if (nbb.isCompletedAbnormally()) {
              //      this.completeExceptionally(nbb.getException());
              //  }
                buffers.addAll(nbb.buffers);
                graphs.addAll(nbb.graphs);
            }
        }
    }
}
