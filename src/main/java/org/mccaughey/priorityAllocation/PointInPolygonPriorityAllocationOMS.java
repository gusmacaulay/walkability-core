package org.mccaughey.priorityAllocation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.mccaughey.utilities.GeoJSONUtilities;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

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
	 */
	@Execute
	public void allocate() {
		Map<String, String> classificationLookup = createLanduseLookup(
				landUseLookupSource, landUseAttribute, priorityAttribute);
		try {
			FeatureIterator<SimpleFeature> regions = regionsOfInterest
					.getFeatures().features();
			// SimpleFeatureCollection intersectingParcels =
			// DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection allocatedParcels = DataUtilities
					.collection(new SimpleFeature[0]);
			try {
				ExecutorService executorService = Executors
						.newFixedThreadPool(Runtime.getRuntime()
								.availableProcessors());
				List<Future> futures = new ArrayList<Future>();
				while (regions.hasNext()) {
					LOGGER.info("Calculating priority allocation for service area ..");
					SimpleFeature regionOfInterest = regions.next();
					Allocater ac = new Allocater(regionOfInterest,
							classificationLookup);
					Future future = executorService.submit(ac);
					futures.add(future);
					// System.out.println("Started .. ");
					// break;
				}
				for (Future future : futures) {

					allocatedParcels.addAll((SimpleFeatureCollection) future
							.get());
					// System.out.println("Completed");
				}
				resultParcels = DataUtilities.source(prioritiseOverlap(
						allocatedParcels, priorityAttribute, priorityOrder));
				resultParcels = DataUtilities
						.source(dissolveByCategory(resultParcels,
								classificationLookup, priorityAttribute));

				// System.out.println("Sourcification Complete");

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
			SimpleFeatureCollection parcels,/*
											 * Map<String, String>
											 * priorityLookup,
											 */String categoryAttribute,
			Map<String, Integer> priorityOrder) {
		SimpleFeatureIterator parcelIterator = parcels.features();
		SimpleFeatureSource parcelSource = DataUtilities.source(parcels);
		Map<String, SimpleFeature> uniqueParcels = new HashMap();
		try {
			while (parcelIterator.hasNext()) {
				SimpleFeature parcel = parcelIterator.next();
				SimpleFeatureCollection intersectingParcels = intersection(
						parcelSource, parcel);
				// System.out.println("Intersecting: " +
				// intersectingParcels.size());
				// if (intersectingParcels.size() == 1) {
				// uniqueParcels.put(parcel.getID(),parcel);
				// }
				// Check if parcel is already in unique parcels before adding
				if (!(uniqueParcels.containsKey(parcel.getID()))) {
					uniqueParcels.put(parcel.getID(), parcel);
				}
				// else {
				SimpleFeatureIterator intersectingParcelsIter = intersectingParcels
						.features();
				while (intersectingParcelsIter.hasNext()) {
					SimpleFeature intersectingParcel = intersectingParcelsIter
							.next();
					if (!(intersectingParcel.getID().equals(parcel.getID()))) {
						{
							String parcelCategory = (String) parcel.getAttribute(categoryAttribute) ;
							String intersectingParcelCategory = (String) intersectingParcel.getAttribute(categoryAttribute);
							if ((priorityOrder.get(parcelCategory)!=null) && (priorityOrder.get(intersectingParcelCategory)!=null)) {
							//if (!= null) && )!=null)) {
								int parcelPriority = priorityOrder
										.get((String) parcel
												.getAttribute(categoryAttribute)); // priorityLookup.get(parcel.getAttribute(categoryAttribute));
								int intersectingPriority = priorityOrder
										.get((String) intersectingParcel
												.getAttribute(categoryAttribute));// priorityLookup.get(intersectingParcel.getAttribute(categoryAttribute));
								if (parcelPriority > intersectingPriority) { //-->intersectingParcel is more important
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

	private SimpleFeatureCollection dissolveByCategory(
			SimpleFeatureSource parcels,
			Map<String, String> classificationLookup, String categoryAttribute) {

		List<SimpleFeature> dissolved = new ArrayList<SimpleFeature>();
		try {
			Set<String> uniqueClassifications = new HashSet<String>(
					classificationLookup.values());
			// for (Entry e : classificationLookup.entrySet()) {
			for (String classification : uniqueClassifications) {
				LOGGER.info("Dissolving category {}", classification);
				Filter filter = CQL.toFilter(categoryAttribute + "="
						+ classification);
				// + e.getValue());
				SimpleFeatureCollection categoryCollection = parcels
						.getFeatures(filter);
				if (categoryCollection.size() > 0) {
					LOGGER.info("Found {} parcels for classification {}",
							categoryCollection.size(), classification);
					dissolved.addAll(dissolve(categoryCollection));
					// dissolved.addAll(DataUtilities.list((categoryCollection)));
				} else {
					LOGGER.info("No parcels found for classification {}",
							classification);
				}
			}
			return DataUtilities.collection(dissolved);
		} catch (IOException e1) {
			LOGGER.error("Failed dissolve by category process");
		} catch (CQLException e) {
			LOGGER.error("Failed dissolve by category process");
		}
		return null;
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
						new ArrayList<String>(), feature.getID() + n);
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
		// PrecisionModel precision = new PrecisionModel(100); // FIXME: should
		// be
		// configurable
		// GeometryFactory fact = new GeometryFactory(precision);
		Geometry geomColl = fact.createGeometryCollection(geom);
		Geometry union = geomColl.union(); // geomColl.buffer(0.0);
		// double t2 = new Date().getTime();
		// LOGGER.info("Time taken Union: " + (t2 - t1) / 1000);
		return union;
	}

	/**
	 * Performs the priority allocation in a region of interest, according to a
	 * priority lookup. This is called by and executor service
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
		 */
		public SimpleFeatureCollection call() throws IOException {
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
			// GeoJSONUtilities.writeFeatures(DataUtilities.collection(allocatedParcels),
			// new File("test_output/allocated_" + allocatedParcels.hashCode() +
			// ".json"));
			// System.out.println("Dissolving: " + index);
			LOGGER.info("Allocated parcels size: {}", allocatedParcels.size());
			return DataUtilities.collection(allocatedParcels);
			// return dissolveByCategory(DataUtilities.source(DataUtilities
			// .collection(allocatedParcels)), classificationLookup,
			// priorityAttribute);
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
						LOGGER.error("Missing priority value for landuse "
								+ landUse);
					}
				}
				List<String> priorityValue = new ArrayList<String>();
				priorityValue.add(currentPriorityClass);
				return buildFeature(parcelOfInterest, allocatedFT,
						priorityValue);
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

	private Map<String, String> createLanduseLookup(URL csvTable,
			String keyColumn, String valueColumn) {
		CSVReader reader;
		Map<String, String> lookupTable = new HashMap<String, String>();

		try {
			// reader = new CSVReader(new FileReader(csvTable.getFile()));
			reader = new CSVReader(new InputStreamReader(new FileInputStream(
					(csvTable.getFile())), "UTF-8"));
			// Assume column names in first line ...
			String[] header = reader.readNext();
			List<String> newAttrs = new ArrayList<String>();
			// List of new attribute names (columns - join column)
			for (String attribute : header) {
				if (!attribute.equals(keyColumn)) {
					// System.out.println(attribute);
					newAttrs.add(attribute);
				}
			}

			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				String key = null;
				String value = null;
				for (int i = 0; i < nextLine.length; i++) { // FIXME: this only
															// works
															// for two columns
					if (header[i].equals(keyColumn)) {
						key = nextLine[i];
					} else if (header[i].equals(valueColumn)) {
						value = nextLine[i];
					}
				}
				lookupTable.put(key, value);
			}
			return lookupTable;
		} catch (FileNotFoundException e) {
			LOGGER.error("Failed to read CSV input, file not found");
		} catch (IOException e) {
			LOGGER.error("Failed to read CSV input");
		}
		return lookupTable;
	}
}
