package org.mccaughey.priorityAllocation;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import oms3.annotations.Execute;
import oms3.annotations.In;
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
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class PointInPolygonPriorityAllocationOMS {
	static final Logger LOGGER = LoggerFactory.getLogger(PointInPolygonPriorityAllocationOMS.class);

	/**
	 * Region(s) of Interest, this is used to filter down and avoid processing data which isn't needed
	 */
	@In
	public SimpleFeatureSource regionsOfInterest;

	/**
	 * A land parcel type data set (eg. cadastre)
	 */
	@In
	public SimpleFeatureSource parcels;

	/**
	 * Point features which will be used to reallocate parcel land use types
	 */
	@In
	public SimpleFeatureSource pointFeatures;

	/**
	 * Attribute in pointFeatures which represents land use type
	 */
	@In
	public String landUseAttribute;

	/**
	 * Attribute in mapping table which maps landUse attribute to priority
	 */
	@In
	public String priorityAttribute;

	@In
	public URL csvTable;
	/**
	 * The priority list of which land uses, to figure out which one to allocate
	 */
	@In
	public List<String> landUsePriorityList;

	@In
	public SortOrder priorityOrder;
	/**
	 * The resulting parcels with re-allocated land use types
	 */
	@Out
	public SimpleFeatureCollection resultParcels;

	/**
	 * Reads in the population count layer and regions layer from given URLs, writes out average density results to
	 * resultsURL
	 */
	@Execute
	public void allocate() {
		Map<String, Integer> priorityLookup = createLanduseLookup(csvTable, landUseAttribute, priorityAttribute);
		try {
			FeatureIterator<SimpleFeature> regions = regionsOfInterest.getFeatures().features();
			SimpleFeatureCollection intersectingParcels = DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection allocatedParcels = DataUtilities.collection(new SimpleFeature[0]);
			try {
				int count = 0;
				ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				List<Future> futures = new ArrayList<Future>();
				while (regions.hasNext()) {
					SimpleFeature regionOfInterest = regions.next();
					Allocater ac = new Allocater(regionOfInterest, priorityLookup, ++count);
					Future future = executorService.submit(ac);
					futures.add(future);
					System.out.println("Started .. " + count);
					//break;
				}
				for (Future future : futures) {
					
					allocatedParcels.addAll((SimpleFeatureCollection) future.get());
					System.out.println("Completed");
				}
				resultParcels = allocatedParcels;
				System.out.println("Sourcification Complete");

			} catch (ExecutionException e) {
				LOGGER.error("Failed to complete process for all features");
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				regions.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private SimpleFeatureCollection dissolveByCategory(SimpleFeatureSource parcels, Map<String, Integer> priorityLookup, String categoryAttribute) {

		List<SimpleFeature> dissolved = new ArrayList();
		try {
			for (Entry e : priorityLookup.entrySet()) {
				Filter filter = CQL.toFilter(categoryAttribute + "=" + e.getValue());
				SimpleFeatureCollection categoryCollection = parcels.getFeatures(filter);
				if (categoryCollection.size() > 0) {
					dissolved.addAll(dissolve(categoryCollection));
				}
			}
			return DataUtilities.collection(dissolved);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (CQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private List<SimpleFeature> dissolve(SimpleFeatureCollection collection) throws IOException {
		FeatureIterator<SimpleFeature> features = collection.features();
		List<SimpleFeature> dissolvedFeatures = new ArrayList<SimpleFeature>();
		try {
			List<Geometry> geometries = new ArrayList();
			SimpleFeature feature = null;
			while (features.hasNext()) {
				feature = features.next();
				geometries.add((Geometry) feature.getDefaultGeometry());
			}
			Geometry dissolved = union(geometries);
			for (int n = 0; n < dissolved.getNumGeometries(); n++) {
				Geometry split = dissolved.getGeometryN(n);
				SimpleFeature splitFeature = buildFeatureFromGeometry(feature, feature.getFeatureType(),split, new ArrayList(),feature.getID()+String.valueOf(n));
			//	splitFeature.setDefaultGeometry(split);
				dissolvedFeatures.add(splitFeature);
			}
			return dissolvedFeatures;

		} finally {
			features.close();
		}

	}

	private SimpleFeature buildFeature(SimpleFeature baseFeature, SimpleFeatureType newFT, List<String> newValues) {
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFT);
		sfb.addAll(baseFeature.getAttributes());
		for (String value : newValues) {
			sfb.add(value);
		}
		return sfb.buildFeature(baseFeature.getID());

	}
	
	private static SimpleFeature buildFeatureFromGeometry(SimpleFeature baseFeature, SimpleFeatureType newFT,Geometry geom, List<String> newValues,String id) {
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFT);
		sfb.addAll(baseFeature.getAttributes());
		sfb.set(sfb.getFeatureType().getGeometryDescriptor().getLocalName(),geom);
		for (String value : newValues) {
			sfb.add(value);
		}
		return sfb.buildFeature(id);

		//turn sfb.buildFeature(id);
	}

	private Geometry union(List geometries) {
		double t1 = new Date().getTime();
		Geometry[] geom = new Geometry[geometries.size()];
		geometries.toArray(geom);
		GeometryFactory fact = geom[0].getFactory();
		//PrecisionModel precision = new PrecisionModel(100); // FIXME: should be configurable
		//GeometryFactory fact = new GeometryFactory(precision);
		Geometry geomColl = fact.createGeometryCollection(geom);
		Geometry union = geomColl.union(); //geomColl.buffer(0.0);
		double t2 = new Date().getTime();
		LOGGER.info("Time taken Union: " + (t2 - t1) / 1000);
		return union;
	}

	class Allocater implements Callable {

		private SimpleFeature regionOfInterest;
		private Map<String, Integer> priorityLookup;
		private int index;

		Allocater(SimpleFeature regionOfInterest, Map<String, Integer> priorityLookup, int index) {
			this.regionOfInterest = regionOfInterest;
			this.priorityLookup = priorityLookup;
			this.index = index;
		}

		public SimpleFeatureCollection call() throws Exception {
			//Do an intersection of parcels with service areas
			SimpleFeatureCollection intersectingParcels = intersection(parcels, regionOfInterest);
			//Priority allocation
			//for each intersecting parcel
			FeatureIterator<SimpleFeature> unAllocatedParcels = intersectingParcels.features();
			List<SimpleFeature> allocatedParcels = new ArrayList();
			while (unAllocatedParcels.hasNext()) {
				SimpleFeature parcel = unAllocatedParcels.next();
				//intersect with points
				SimpleFeature allocatedParcel = allocateParcel(pointFeatures, parcel, priorityLookup);
				allocatedParcels.add(allocatedParcel);
			}
			System.out.println("Dissolving: " + index);
			return dissolveByCategory(DataUtilities.source(DataUtilities.collection(allocatedParcels)), priorityLookup, priorityAttribute);
		}

		private SimpleFeature allocateParcel(SimpleFeatureSource priorityFeatures, SimpleFeature parcel, Map<String, Integer> priorityLookup) throws NoSuchElementException, IOException {
			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
			String geometryPropertyName = priorityFeatures.getSchema().getGeometryDescriptor().getLocalName();

			Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(parcel.getDefaultGeometry()));
			//SortBy prioritise = ff.sort(priorityAttribute, SortOrder.ASCENDING);

			SimpleFeatureIterator pFeatures = priorityFeatures.getFeatures(filter).features();
			List<String> additionalAttributes = new ArrayList<String>();
			additionalAttributes.add(priorityAttribute);
			SimpleFeatureType allocatedFT = createNewFeatureType(parcel.getFeatureType(), additionalAttributes);
			try {
				//SimpleFeature priorityFeature = null;
				int currentPriority = -1;
				while (pFeatures.hasNext()) {
					//if (priorityFeature == null)
					//	priorityFeature = pFeatures.next();
					//else {
					SimpleFeature comparisonFeature = pFeatures.next();
					//currentPriority = priorityLookup.get(priorityFeature.getAttribute(landUseAttribute).toString());
					String landUse = comparisonFeature.getAttribute(landUseAttribute).toString();
					try {

						int comparisonPriority = priorityLookup.get(landUse);
						if (comparisonPriority > currentPriority) { //TODO: generalise this for ascending/descending
							currentPriority = comparisonPriority;
						}
					} catch (NullPointerException e) {
						LOGGER.error("Missing priority value for landuse " + landUse);
					}
					//}
				}
				List<String> priorityValue = new ArrayList<String>();
				priorityValue.add(String.valueOf(currentPriority));
				return buildFeature(parcel, allocatedFT, priorityValue);
			} finally {
				pFeatures.close();
			}
		}

		private SimpleFeatureCollection intersection(SimpleFeatureSource featuresOfInterest, SimpleFeature intersectingFeature) throws IOException {
			SimpleFeatureCollection features = featuresOfInterest.getFeatures();
			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
			String geometryPropertyName = features.getSchema().getGeometryDescriptor().getLocalName();

			Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(intersectingFeature.getDefaultGeometry()));

			//	return features.subCollection(filter); <-- THIS IS REALLY SLOW
			return featuresOfInterest.getFeatures(filter); // <-- DO THIS INSTEAD

		}

		private SimpleFeatureType createNewFeatureType(SimpleFeatureType baseFeatureType, List<String> newAttributeNames) {
			//     SimpleFeatureType baseFeatureType = (SimpleFeatureType) baseFeature.getType(); 
			SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
			stb.init(baseFeatureType);
			stb.setName("newFeatureType");
			//Add new attributes to feature type
			for (String attr : newAttributeNames) {
				stb.add(attr, String.class);
			}
			return stb.buildFeatureType();
		}

		private SimpleFeature buildFeature(SimpleFeature baseFeature, SimpleFeatureType newFT, List<String> newValues) {
			SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(newFT);
			sfb.addAll(baseFeature.getAttributes());
			for (String value : newValues) {
				sfb.add(value);
			}
			return sfb.buildFeature(baseFeature.getID());

		}

	}

	private Map<String, Integer> createLanduseLookup(URL csvTable, String keyColumn, String valueColumn) {
		CSVReader reader;
		Map<String, Integer> lookupTable = new HashMap();

		try {
			reader = new CSVReader(new FileReader(csvTable.getFile()));

			//Assume column names in first line ...
			String[] header = reader.readNext();
			List<String> newAttrs = new ArrayList();
			//List of new attribute names (columns - join column)
			for (String attribute : header) {
				if (attribute != keyColumn) {
					System.out.println(attribute);
					newAttrs.add(attribute);
				}
			}

			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				String key = null;
				int value = -1;
				for (int i = 0; i < nextLine.length; i++) {
					if (header[i].toString().equals(keyColumn)) {
						key = nextLine[i];
					} else if (header[i].toString().equals(valueColumn)) {
						value = Integer.parseInt(nextLine[i]);
					}
				}
				lookupTable.put(key, value);
			}
			return lookupTable;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lookupTable;
	}
}
