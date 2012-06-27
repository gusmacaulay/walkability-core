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
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates network buffers for a set of points, using Fork/Join for
 * concurrency
 * 
 * @author amacaulay
 */
public class NetworkBufferBatch extends RecursiveAction {

	static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferBatch.class);
	private SimpleFeatureSource network;
	private SimpleFeatureCollection points;
	private SimpleFeatureCollection buffers;
	private SimpleFeatureCollection graphs;
	private Double distance;
	private Double bufferSize;

	/**
	 * Generates network buffers for a set of points
	 * @param network The network to use to generate service networks
	 * @param points The set of points of interest
	 * @param distance The distance to traverse along the network.
	 * @param bufferSize The length to buffer the service network
	 */
	public NetworkBufferBatch(SimpleFeatureSource network, SimpleFeatureCollection points, Double distance,
			Double bufferSize) {
		this.network = network;
		this.points = points;
		this.distance = distance;
		this.bufferSize = bufferSize;
		this.buffers = FeatureCollections.newCollection();
		this.graphs = FeatureCollections.newCollection();

	}

	/**
	 * 
	 * @return A SimpleFeatureCollection of the service area networks for all points of interest
	 */
	public SimpleFeatureCollection getGraphs() {
		return graphs;
	}

	/**
	 * 
	 * @return A SimpleFeatureCollection consisting of the buffered service areas for each point of interest
	 */
	public SimpleFeatureCollection createBuffers() {
		Runtime runtime = Runtime.getRuntime();
		int nProcessors = runtime.availableProcessors();
		int nThreads = 1;//nProcessors;

		LOGGER.debug("Initialising ForkJoinPool with {}", nThreads);
		// Fork/Join handles threads for me, all I do is invoke
		ForkJoinPool fjPool = new ForkJoinPool(nThreads);
		fjPool.invoke(this);

		return buffers;
	}

	@Override
	protected void compute() {
		if (points.size() == 1) { // if there is only one point compute the
									// network region and store in buffers
			SimpleFeatureIterator features = points.features();
			try {
				SimpleFeature point = features.next();
				LOGGER.info("+");
				Map serviceArea = NetworkBuffer.findServiceArea(network, point, distance, bufferSize);
				if (serviceArea != null) {
					// List<SimpleFeature> networkBuffer =
					// NetworkBuffer.createLinesFromEdges(serviceArea);
					SimpleFeatureCollection graph = DataUtilities.collection(NetworkBuffer
							.createLinesFromEdges(serviceArea));
					LOGGER.info("Points CRS: {}", points.getSchema().getCoordinateReferenceSystem());
					SimpleFeature networkBuffer = NetworkBuffer.createBufferFromEdges(serviceArea, bufferSize, points
							.getSchema().getCoordinateReferenceSystem(), String.valueOf(point.getAttribute("UID")));
					if (networkBuffer != null) {
						buffers.add(networkBuffer); // .addAll(DataUtilities.collection(networkBuffer));
						graphs.addAll(graph);
					}
				}
			} catch (Exception e) {
				this.completeExceptionally(e);
			} finally {
				features.close();
			}
		} else { // otherwise split the points into single point arrays and
					// invokeAll
			ArrayList<NetworkBufferBatch> buffernators = new ArrayList();
			FeatureIterator features = points.features();
			try {
				while (features.hasNext()) {
					SimpleFeatureCollection singlePointCollection = FeatureCollections.newCollection();
					SimpleFeature feature = (SimpleFeature) features.next();
					singlePointCollection.add(feature);
					NetworkBufferBatch nbb = new NetworkBufferBatch(network, singlePointCollection, distance,
							bufferSize);
					buffernators.add(nbb);
				}
			} finally {
				features.close();
			}
			invokeAll(buffernators);
			for (NetworkBufferBatch nbb : buffernators) {
				// if (nbb.isCompletedAbnormally()) {
				// this.completeExceptionally(nbb.getException());
				// }
				buffers.addAll(nbb.buffers);
				graphs.addAll(nbb.graphs);
			}
		}
	}
}
