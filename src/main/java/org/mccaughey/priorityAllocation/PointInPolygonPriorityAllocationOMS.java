package org.mccaughey.priorityAllocation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
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
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

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
	public SimpleFeatureSource regionsOfInterest;

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
		Map<String, String> classificationLookup = createLanduseLookup(
				landUseLookupSource, landUseAttribute, priorityAttribute);
		try {
			FeatureIterator<SimpleFeature> regions = regionsOfInterest
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
					LOGGER.info("Calculating priority allocation for service area ..");
					SimpleFeature regionOfInterest = regions.next();
					Allocater ac = new Allocater(regionOfInterest,
							classificationLookup);
					Future<List<SimpleFeature>> future = executorService
							.submit(ac);
					futures.add(future);
				}
				for (Future<List<SimpleFeature>> future : futures) {
					allocatedParcels.addAll(future.get());
				}
				resultParcels = DataUtilities.source(prioritiseOverlap(
						DataUtilities.collection(allocatedParcels),
						priorityAttribute, priorityOrder));
				resultParcels = DataUtilities.source(DataUtilities
						.collection(dissolveByCategory(
								DataUtilities.collection(allocatedParcels),
								classificationLookup, priorityAttribute)));

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

	private SimpleFeatureCollection intersection(
			SimpleFeatureSource featuresOfInterest,
			SimpleFeature intersectingFeature) throws IOException {
		SimpleFeatureCollection features = featuresOfInterest.getFeatures();
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		String geometryPropertyName = features.getSchema()
				.getGeometryDescriptor().getLocalName();

		Filter filter = ff.intersects(ff.property(geometryPropertyName),
				ff.literal(intersectingFeature.getDefaultGeometry()));

		// return features.subCollection(filter); <-- THIS IS REALLY SLOW
		return featuresOfInterest.getFeatures(filter); // <-- DO THIS INSTEAD

	}

	private SimpleFeatureCollection prioritiseOverlap(
			SimpleFeatureCollection parcels, String categoryAttribute,
			Map<String, Integer> priorityOrder) {
		SimpleFeatureIterator parcelIterator = parcels.features();
		SimpleFeatureSource parcelSource = DataUtilities.source(parcels);
		Map<String, SimpleFeature> uniqueParcels = new HashMap();
		try {
			while (parcelIterator.hasNext()) {
				SimpleFeature parcel = parcelIterator.next();
				SimpleFeatureCollection intersectingParcels = intersection(
						parcelSource, parcel);

				if (!(uniqueParcels.containsKey(parcel.getID()))) {
					uniqueParcels.put(parcel.getID(), parcel);
				}
				SimpleFeatureIterator intersectingParcelsIter = intersectingParcels
						.features();
				while (intersectingParcelsIter.hasNext()) {
					SimpleFeature intersectingParcel = intersectingParcelsIter
							.next();
					if (!(intersectingParcel.getID().equals(parcel.getID()))) {
						{
							String parcelCategory = (String) parcel
									.getAttribute(categoryAttribute);
							String intersectingParcelCategory = (String) intersectingParcel
									.getAttribute(categoryAttribute);
							if ((priorityOrder.get(parcelCategory) != null)
									&& (priorityOrder
											.get(intersectingParcelCategory) != null)) {
								// if (!= null) && )!=null)) {
								int parcelPriority = priorityOrder
										.get((String) parcel
												.getAttribute(categoryAttribute)); // priorityLookup.get(parcel.getAttribute(categoryAttribute));
								int intersectingPriority = priorityOrder
										.get((String) intersectingParcel
												.getAttribute(categoryAttribute));// priorityLookup.get(intersectingParcel.getAttribute(categoryAttribute));
								if (parcelPriority > intersectingPriority) { // -->intersectingParcel
																				// is
																				// more
																				// important
									Geometry parcelGeometry = (Geometry) uniqueParcels
											.get(parcel.getID())
											.getDefaultGeometry();
									Geometry intersectingGeometry = (Geometry) intersectingParcel
											.getDefaultGeometry();
									Geometry parcelDifference = parcelGeometry
											.difference(intersectingGeometry);
									SimpleFeature newFeature = buildFeatureFromGeometry(
											parcel, parcelDifference);
									uniqueParcels.put(parcel.getID(),
											newFeature);
								}
							}
						}
					}
				}
			}
		} catch (IOException e) {
			LOGGER.error("Failed at performing overlap removal process");
		} finally {
			parcelIterator.close();
		}
		Collection<SimpleFeature> features = uniqueParcels.values();
		return DataUtilities.collection(new ArrayList(features));
	}

	private List<SimpleFeature> dissolveByCategory(
			SimpleFeatureCollection parcels,
			Map<String, String> classificationLookup, String categoryAttribute)
			throws IOException, CQLException {

		List<SimpleFeature> dissolved = new ArrayList<SimpleFeature>();
		try {
			Set<String> uniqueClassifications = new HashSet<String>(
					classificationLookup.values());
			SimpleFeatureSource parcelsSource = DataUtilities.source(parcels);
			for (String classification : uniqueClassifications) {
				Filter filter = CQL.toFilter(categoryAttribute + "="
						+ classification);
				// LOGGER.info(filter.toString());
				// + e.getValue());
				SimpleFeatureCollection categoryCollection = parcelsSource
						.getFeatures(filter);
				if (categoryCollection.size() > 0) {
					List<SimpleFeature> sc = dissolve(categoryCollection);
					dissolved.addAll(sc);

					// dissolved.addAll(DataUtilities.list((categoryCollection)));
				}
			}
			return dissolved;
			// return DataUtilities.collection(dissolved);
		} catch (IOException e1) {
			throw new IOException("Failed dissolve by category process", e1);
		} catch (CQLException e) {
			throw new CQLException("Failed dissolve by category process: "
					+ e.getMessage());
		}
	}

	private List<SimpleFeature> dissolve(SimpleFeatureCollection collection)
			throws IOException {
		FeatureIterator<SimpleFeature> features = collection.features();
		List<SimpleFeature> dissolvedFeatures = new ArrayList<SimpleFeature>();
		try {
			List<Geometry> geometries = new ArrayList<Geometry>();
			SimpleFeature feature = null;
			while (features.hasNext()) {
				feature = features.next();
				geometries.add((Geometry) feature.getDefaultGeometry());
			}
			Geometry dissolved = union(geometries);
			for (int n = 0; n < dissolved.getNumGeometries(); n++) {
				Geometry split = dissolved.getGeometryN(n);
				SimpleFeature splitFeature = buildFeatureFromGeometry(feature,
						feature.getFeatureType(), split,
						new ArrayList<String>(), "id." + collection.hashCode()
								+ "." + n);
				// splitFeature.setDefaultGeometry(split);
				dissolvedFeatures.add(splitFeature);
			}
			return dissolvedFeatures;

		} finally {
			features.close();
		}

	}

	private static SimpleFeature buildFeatureFromGeometry(
			SimpleFeature baseFeature, Geometry geom) {
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(
				baseFeature.getFeatureType());
		sfb.addAll(baseFeature.getAttributes());
		sfb.set(sfb.getFeatureType().getGeometryDescriptor().getLocalName(),
				geom);
		return sfb.buildFeature(baseFeature.getID());
	}

	private static SimpleFeature buildFeatureFromGeometry(
			SimpleFeature baseFeature, SimpleFeatureType newFT, Geometry geom,
			List<String> newValues, String id) {
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFT);
		sfb.addAll(baseFeature.getAttributes());
		sfb.set(sfb.getFeatureType().getGeometryDescriptor().getLocalName(),
				geom);
		for (String value : newValues) {
			sfb.add(value);
		}
		return sfb.buildFeature(id);
	}

	private Geometry union(List<Geometry> geometries) {
		// double t1 = new Date().getTime();
		Geometry[] geom = new Geometry[geometries.size()];
		geometries.toArray(geom);
		GeometryFactory fact = geom[0].getFactory();
		Geometry geomColl = fact.createGeometryCollection(geom);
		Geometry union = geomColl.union(); // geomColl.buffer(0.0);
		return union;
	}

	/**
	 * Performs the priority allocation in a region of interest, according to a
	 * priority lookup. This is called by an executor service
	 * 
	 * @author amacaulay
	 * 
	 */
	class Allocater implements Callable {

		private SimpleFeature regionOfInterest;
		private Map<String, String> classificationLookup;

		Allocater(SimpleFeature regionOfInterest,
				Map<String, String> priorityLookup) {
			this.regionOfInterest = regionOfInterest;
			this.classificationLookup = priorityLookup;
		}

		/**
		 * Does and intersection of parcels with the region of interest and then
		 * allocates each parcel.
		 * 
		 * @throws CQLException
		 */
		public List<SimpleFeature> call() throws IOException, CQLException {
			// Do an intersection of parcels with service areas
			SimpleFeatureCollection intersectingParcels = intersection(parcels,
					regionOfInterest);
			// Priority allocation
			// for each intersecting parcel
			FeatureIterator<SimpleFeature> unAllocatedParcels = intersectingParcels
					.features();
			// GeoJSONUtilities.writeFeatures(intersectingParcels, new
			// File("test_output/intersecting_" + intersectingParcels.hashCode()
			// + ".json"));
			List<SimpleFeature> allocatedParcels = new ArrayList<SimpleFeature>();
			while (unAllocatedParcels.hasNext()) {
				SimpleFeature parcel = unAllocatedParcels.next();
				// intersect with points
				SimpleFeature allocatedParcel = allocateParcel(pointFeatures,
						parcel, classificationLookup, priorityOrder);
				allocatedParcels.add(allocatedParcel);
			}
			unAllocatedParcels.close();
			// GeoJSONUtilities.writeFeatures(DataUtilities
			// .collection(allocatedParcels), new File(
			// "test_output/allocated_" + allocatedParcels.hashCode()
			// + ".json"));

			SimpleFeatureCollection allocatedFeatures = DataUtilities
					.collection(allocatedParcels);
			SimpleFeatureSource source = DataUtilities
					.source(prioritiseOverlap(allocatedFeatures,
							priorityAttribute, priorityOrder));
			return dissolveByCategory(source.getFeatures(),
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
			SimpleFeatureType allocatedFT = createNewFeatureType(
					parcelOfInterest.getFeatureType(), additionalAttributes);
			try {
				int currentPriority = priorityOrder.size() + 1;
				String currentPriorityClass = "";
				while (pointFeatures.hasNext()) {
					SimpleFeature comparisonPoint = pointFeatures.next();

					String landUse = comparisonPoint.getAttribute(
							landUseAttribute).toString();

					try {
						String priorityClass = String
								.valueOf(classificationLookup.get(landUse));

						// LOGGER.info("Comparison Feature LandUse " + landUse);
						int comparisonPriority = priorityOrder
								.get(priorityClass);
						if (comparisonPriority < currentPriority) {
							currentPriority = comparisonPriority;
							currentPriorityClass = priorityClass;
						}
					} catch (NullPointerException e) {
						// LOGGER.info("Missing priority value for landuse "
						// + landUse);
					}
				}
				if (currentPriorityClass != "") {
					List<String> priorityValue = new ArrayList<String>();
					priorityValue.add(currentPriorityClass);
					return buildFeature(parcelOfInterest, allocatedFT,
							priorityValue);
				} else { // don't add parcels which have no classification of
							// interest
					return null;
				}
			} finally {
				pointFeatures.close();
			}
		}

		private SimpleFeatureType createNewFeatureType(
				SimpleFeatureType baseFeatureType,
				List<String> newAttributeNames) {
			// SimpleFeatureType baseFeatureType = (SimpleFeatureType)
			// baseFeature.getType();
			SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
			stb.init(baseFeatureType);
			stb.setName("newFeatureType");
			// Add new attributes to feature type
			for (String attr : newAttributeNames) {
				stb.add(attr, String.class);
			}
			return stb.buildFeatureType();
		}

		private SimpleFeature buildFeature(SimpleFeature baseFeature,
				SimpleFeatureType newFT, List<String> newValues) {
			SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFT);
			sfb.addAll(baseFeature.getAttributes());
			for (String value : newValues) {
				sfb.add(value);
			}
			return sfb.buildFeature(baseFeature.getID());
		}

	}

	private Map<String, String> createLanduseLookup(
			SimpleFeatureSource lookupSource, String keyColumn,
			String valueColumn) {
		SimpleFeatureIterator lookupFeatures;
		Map<String, String> lookupTable = new HashMap<String, String>();
		try {
			lookupFeatures = lookupSource.getFeatures().features();
			while (lookupFeatures.hasNext()) {
				SimpleFeature lookupFeature = lookupFeatures.next();
				lookupTable.put((String) lookupFeature.getAttribute(keyColumn),
						(String) lookupFeature.getAttribute(valueColumn));
			}
		} catch (IOException e) {
			LOGGER.error("Failed to read SimpleFeaturSource input");
		}
		return lookupTable;
	}
}
