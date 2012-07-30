package org.mccaughey.density;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class NettDensityOMS {
	static final Logger LOGGER = LoggerFactory.getLogger(NettDensityOMS.class);

	/**
	 * The input regions to calculate density for
	 */
	@In
	public SimpleFeatureSource regionsOfInterest;

	/**
	 * A land parcel type data set (eg. cadastre)
	 */
	@In
	public SimpleFeatureSource parcels;

	/**
	 * Residential points data set (for figuring out what is/isn't residential)
	 */
	@In
	public SimpleFeatureSource residentialPoints;

	/**
	 * The resulting regions with average density calculated
	 */
	@Out
	public SimpleFeatureSource resultsSource;

	/**
	 * Reads in the population count layer and regions layer from given URLs, writes out average density results to
	 * resultsURL
	 */
	@Execute
	public void nettDensity() {
		try {

			FeatureIterator<SimpleFeature> regions = regionsOfInterest.getFeatures().features();
			SimpleFeatureCollection intersectingFeatures = DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection dissolvedParcels = DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection pipFeatures = DataUtilities.collection(new SimpleFeature[0]);
			try {
				while (regions.hasNext()) {
					SimpleFeature regionOfInterest = regions.next();
					//Do an intersection of parcels with service areas
					intersectingFeatures = intersection(parcels, regionOfInterest);

					//Do an point in polygon intersection parcel/service with residential points
					pipFeatures = pipIntersection(residentialPoints.getFeatures(), DataUtilities.source(intersectingFeatures));
					//Dissolve parcel/service intersection
					LOGGER.info("Attempting Dissolvv ...");
				//	SimpleFeature dissolvedParcel = dissolve(intersectingFeatures, regionOfInterest);
					dissolvedParcels.addAll(intersectingFeatures);
					//Dissolve parcel/residential intersection
					SimpleFeature dissolvedResidential = dissolve(pipFeatures, regionOfInterest);
					//Calculate proportion(density) of parcel/service:parcel/residential
					Double residentialArea = getTotalArea(pipFeatures);
					Double parcelArea = getTotalArea(intersectingFeatures);
					System.out.println("Density: " + residentialArea / parcelArea);
					//break;
				}
				System.out.print("Processing Complete...");
				resultsSource = DataUtilities.source(dissolvedParcels);
				System.out.println("Found features" + dissolvedParcels.size());

			} catch (Exception e) {
				LOGGER.error("Failed to complete process for all features");
				e.printStackTrace();
			} finally {
				regions.close();
			}

		} catch (IOException e) {
			LOGGER.error("Failed to read input/s");
		}
	}

	private SimpleFeature dissolve(SimpleFeatureCollection collection, SimpleFeature parent) throws IOException {
		FeatureIterator<SimpleFeature> features = collection.features();
		try {

			List<Geometry> geometries = new ArrayList();
			SimpleFeature feature = null;
			while (features.hasNext()) {
				//	System.out.println("Doing some stuff ..");
				feature = features.next();
				geometries.add((Geometry) feature.getDefaultGeometry());
			}
			Geometry dissolved = union(geometries);
			return buildFeatureFromGeometry(parent.getFeatureType(), dissolved, parent.getID());
		} finally {
			features.close();
		}

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
		System.out.println("Time taken Union: " + (t2 - t1) / 1000);
		return union;
	}

	private Geometry union_better(List<Geometry> geometries) {

		// int loopcount = 0;
		PrecisionModel precision = new PrecisionModel(10); // FIXME: should be configurable
		GeometryFactory fact = new GeometryFactory(precision);
		Geometry all = fact.createGeometry(null);
		double t1 = new Date().getTime();
		while (geometries.size() > 0) {
			// LOGGER.info("loopcount: {}",loopcount++);
			List<Geometry> unjoined = new ArrayList();
			for (Geometry geom : geometries) {
				//Geometry geom = (Geometry) ((SimpleFeature) serviceArea.get(edge)).getDefaultGeometry();
				// LOGGER.info("GEOM TYPE: {}",geom.getGeometryType());
				geom = geom.union();
				// LOGGER.info("Unioned collection");
				//geom = geom.buffer(distance);
				// LOGGER.info("Buffered geom");
				try {
					if (all != null) {
						all = all.union().union();
					}
					if (all == null) {
						all = geom;
					} else if (!(all.covers(geom))) {
						// LOGGER.info("ALL TYPE: {} GEOM TYPE: {}",
						// all.getGeometryType(), geom.getGeometryType());
						if (all.intersects(geom)) {
							all = all.union(geom);
						} else {
							// LOGGER.info("No intersection ...");
							unjoined.add(geom);
						}
					}
				} catch (Exception e) {
					if (e.getMessage().contains("non-noded")) {
						LOGGER.info(e.getMessage());
					} else {
						LOGGER.error("Failed to create buffer from network: " + e.getMessage());
						return null;
					}
				}
			}
			geometries = unjoined;
		}
		double t2 = new Date().getTime();
		System.out.println("Time taken Union: " + (t2 - t1) / 1000);
		return all;
	}

	private Double getTotalArea(SimpleFeatureCollection features) {
		double area = 0.0;
		SimpleFeatureIterator iter = features.features();
		while (iter.hasNext()) {
			area += ((Geometry) (iter.next().getDefaultGeometry())).getArea();
		}
		return area;
	}

	private static SimpleFeature buildFeatureFromGeometry(SimpleFeatureType featureType, Geometry geom, String id) {

		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
		stb.init(featureType);
		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(featureType);
		sfb.add(id);
		sfb.add(geom);

		return sfb.buildFeature(id);
	}

	private SimpleFeatureCollection pipIntersection(SimpleFeatureCollection points, SimpleFeatureSource regions) throws IOException {
		SimpleFeatureIterator pointsIter = points.features();
		SimpleFeatureCollection pipFeatures = DataUtilities.collection(new SimpleFeature[0]);
		try {
			while (pointsIter.hasNext()) {
				SimpleFeature point = pointsIter.next();
				pipFeatures.addAll(intersection(regions, point));
			}
			return pipFeatures;
		} finally {
			pointsIter.close();
		}
	}

	private SimpleFeatureCollection intersection(SimpleFeatureSource featuresOfInterest, SimpleFeature intersectingFeature) throws IOException {

		SimpleFeatureCollection features = featuresOfInterest.getFeatures();
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		String geometryPropertyName = features.getSchema().getGeometryDescriptor().getLocalName();

		Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(intersectingFeature.getDefaultGeometry()));

		//	return features.subCollection(filter); --> THIS IS REALLY SLOW
		return featuresOfInterest.getFeatures(filter); // --> DO THIS INSTEAD

	}
}
