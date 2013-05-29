package org.mccaughey.priorityAllocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs the priority allocation in a region of interest, according to a
 * priority lookup. This is called by an executor service
 * 
 * @author amacaulay
 * 
 */
class Allocater implements Callable<List<SimpleFeature>> {

	static final Logger LOGGER = LoggerFactory.getLogger(Allocater.class);

	private SimpleFeature regionOfInterest;
	private Map<String, String> classificationLookup;
	private SimpleFeatureSource pointFeatures;
	private Map<String, Integer> priorityOrder;
	private SimpleFeatureSource parcels;
	private String priorityAttribute;
	private String landUseAttribute;

	Allocater(SimpleFeature regionOfInterest,
			Map<String, String> priorityLookup,
			SimpleFeatureSource pointFeatures,
			Map<String, Integer> priorityOrder, SimpleFeatureSource parcels,
			String priorityAttribute, String landUseAttribute) {
		this.regionOfInterest = regionOfInterest;
		this.classificationLookup = priorityLookup;
		this.pointFeatures = pointFeatures;
		this.priorityOrder = priorityOrder;
		this.parcels = parcels;
		this.priorityAttribute = priorityAttribute;
		this.landUseAttribute = landUseAttribute;
	}

	/**
	 * Does and intersection of parcels with the region of interest and then
	 * allocates each parcel.
	 * 
	 * @throws CQLException
	 */
	public List<SimpleFeature> call() throws IOException, CQLException {
		// Do an intersection of parcels with service areas
		SimpleFeatureCollection intersectingParcels = AllocationUtils
				.intersection(parcels, regionOfInterest);
		// Priority allocation
		// for each intersecting parcel
		FeatureIterator<SimpleFeature> unAllocatedParcels = intersectingParcels
				.features();

		List<SimpleFeature> allocatedParcels = new ArrayList<SimpleFeature>();
		while (unAllocatedParcels.hasNext()) {
			SimpleFeature parcel = unAllocatedParcels.next();
			
			SimpleFeature allocatedParcel = allocateParcel(pointFeatures,
					parcel, classificationLookup, priorityOrder);
			allocatedParcels.add(allocatedParcel);
		}
		unAllocatedParcels.close();

		SimpleFeatureCollection allocatedFeatures = DataUtilities
				.collection(allocatedParcels);
		SimpleFeatureSource source = DataUtilities.source(AllocationUtils
				.prioritiseOverlap(allocatedFeatures, priorityAttribute,
						priorityOrder));
		return AllocationUtils.dissolveByCategory(source.getFeatures(),
				classificationLookup, priorityAttribute);
	}

	private SimpleFeature allocateParcel(
			SimpleFeatureSource pointPriorityFeatures,
			SimpleFeature parcelOfInterest,
			Map<String, String> classificationLookup,
			Map<String, Integer> priorityOrder) throws IOException {
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		String geometryPropertyName = pointPriorityFeatures.getSchema()
				.getGeometryDescriptor().getLocalName();

		Filter filter = ff.intersects(ff.property(geometryPropertyName),
				ff.literal(parcelOfInterest.getDefaultGeometry()));
		// SortBy prioritise = ff.sort(priorityAttribute,
		// SortOrder.ASCENDING);

		SimpleFeatureIterator pointFeatures = pointPriorityFeatures
				.getFeatures(filter).features();
		List<String> additionalAttributes = new ArrayList<String>();
		additionalAttributes.add(priorityAttribute);
		SimpleFeatureType allocatedFT = AllocationUtils.createNewFeatureType(
				parcelOfInterest.getFeatureType(), additionalAttributes);
		try {
			int currentPriority = priorityOrder.size() + 1;
			String currentPriorityClass = "";
			while (pointFeatures.hasNext()) {
				SimpleFeature comparisonPoint = pointFeatures.next();

				String landUse = comparisonPoint.getAttribute(landUseAttribute)
						.toString();

				if (classificationLookup.containsKey(landUse)) {
					String priorityClass = String.valueOf(classificationLookup
							.get(landUse));
					if (priorityOrder.containsKey(priorityClass)) {
						int comparisonPriority = priorityOrder
								.get(priorityClass);
						if (comparisonPriority < currentPriority) {
							currentPriority = comparisonPriority;
							currentPriorityClass = priorityClass;
						}
					} else {
						LOGGER.debug("Misssing priority value for classification "
								+ priorityClass);
					}
				} else {
					LOGGER.debug("Missing classification value for landuse "
							+ landUse);
				}
			}
			if (currentPriorityClass != "") {
				List<String> priorityValue = new ArrayList<String>();
				priorityValue.add(currentPriorityClass);
				return AllocationUtils.buildFeature(parcelOfInterest,
						allocatedFT, priorityValue);
			} else { // don't add parcels which have no classification of
						// interest
				return null;
			}
		} finally {
			pointFeatures.close();
		}
	}
}
