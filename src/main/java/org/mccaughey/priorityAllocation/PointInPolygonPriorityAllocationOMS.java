package org.mccaughey.priorityAllocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Name;
import oms3.annotations.Out;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class performs the point in polygon priority allocation process, for
 * allocating land use types to cadastral/parcel data
 * 
 * @author amacaulay
 * 
 */
@Name("Priority Allocation")
@Description("The Point in Polygon Priority Allocation process creates a land use polygon data set based on point features,parcel features and a classification priority")
public class PointInPolygonPriorityAllocationOMS {
	static final Logger LOGGER = LoggerFactory
			.getLogger(PointInPolygonPriorityAllocationOMS.class);

	/**
	 * Region(s) of Interest, this is used to filter down and avoid processing
	 * data which isn't needed
	 */
	@In
	@Name("Neighbourhoods")
	@Description("The extent of the neighbourhoods are used to limit the analysis extent")
	public SimpleFeatureSource regionsSource;

	/**
	 * A land parcel type data set (eg. cadastre)
	 */
	@In
	@Name("Parcels")
	@Description("Cadastral parcels used to provide an areal extent to resulting land use polygons")
	public SimpleFeatureSource parcels;

	/**
	 * Point features which will be used to reallocate parcel land use types
	 */
	@In
	@Name("Land Use Feature Points")
	@Description("Point feature data set with land use categories stored ina an attribute")
	public SimpleFeatureSource pointFeatures;

	/**
	 * Attribute in pointFeatures which represents land use type
	 */
	@In
	@Name("Land Use Attribute")
	@Description("The land use attribute in the point data set which will be used to allocate land uses to the parcels")
	public String landUseAttribute;

	/**
	 * Attribute in mapping table which maps landUse attribute to priority
	 */
	@In
	@Name("Priority Attribute - $$$$$$$$$$$$$$$$$$ THIS WILL GO TO JOIN TOOL")
	@Description("The Attribute on which to apply the priority order")
	public String priorityAttribute;

	/**
	 * The priority list of which land uses, to figure out which one to allocate
	 */
	@In
	@Name("Priority Order")
	@Description("An ordered list of land use categories that will be used to allocate a single land use to parcels where multiple category types may exisit within them.")
	public Map<String, Integer> priorityOrder;

	@In
	@Name("Land Use Lookup")
	@Description("A lookup table with columns matching land use attributes to classifications")
	public SimpleFeatureSource landUseLookupSource;

	/**
	 * The resulting parcels with re-allocated land use types
	 */
	@Out
	public SimpleFeatureSource resultParcels;

	/**
	 * Reads in the population count layer and regions layer from given URLs,
	 * writes out average density results to resultsURL
	 * 
	 * @throws CQLException
	 */
	@Execute
	public void allocate() throws CQLException {
		LOGGER.info("Calculating Priority Allocation");
		Map<String, String> classificationLookup = AllocationUtils
				.createLanduseLookup(landUseLookupSource, landUseAttribute,
						priorityAttribute);
		try {
			FeatureIterator<SimpleFeature> regions = regionsSource
					.getFeatures().features();
			// SimpleFeatureCollection intersectingParcels =
			// DataUtilities.collection(new SimpleFeature[0]);
			List<SimpleFeature> allocatedParcels = new ArrayList<SimpleFeature>();
			try {
				ExecutorService executorService = Executors
						.newFixedThreadPool(Runtime.getRuntime()
								.availableProcessors());
				// ExecutorService executorService = Executors
				// .newFixedThreadPool(1);
				List<Future<List<SimpleFeature>>> futures = new ArrayList<Future<List<SimpleFeature>>>();
				while (regions.hasNext()) {
					LOGGER.debug("Calculating priority allocation for service area ..");
					SimpleFeature regionOfInterest = regions.next();
					Allocater ac = new Allocater(regionOfInterest,
							classificationLookup, pointFeatures, priorityOrder,
							parcels, priorityAttribute, landUseAttribute);
					Future<List<SimpleFeature>> future = executorService
							.submit(ac);
					futures.add(future);
				}
				for (Future<List<SimpleFeature>> future : futures) {
					allocatedParcels.addAll(future.get());
				}
				resultParcels = DataUtilities.source(AllocationUtils
						.prioritiseOverlap(
								DataUtilities.collection(allocatedParcels),
								priorityAttribute, priorityOrder));
				resultParcels = DataUtilities.source(DataUtilities
						.collection(AllocationUtils.dissolveByCategory(
								DataUtilities.collection(allocatedParcels),
								classificationLookup, priorityAttribute)));
				LOGGER.info("Completed Priority Allocation");

			} catch (ExecutionException e) {
				LOGGER.error(
						"Failed to complete process for all features; ExecutionException: {}",
						e.getMessage());
			} catch (InterruptedException e) {
				LOGGER.error(
						"Failed to complete process for all features; InterruptedException: {}",
						e.getMessage());
			} finally {
				regions.close();
			}
		} catch (IOException e) {
			LOGGER.error("Failed to read features");
		}

	}

}
