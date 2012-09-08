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
			SimpleFeatureCollection intersectingFeatures;// = DataUtilities.collection(new SimpleFeature[0]);
			List<SimpleFeature> densityFeatures = new ArrayList();// = DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection pipFeatures;// = DataUtilities.collection(new SimpleFeature[0]);
			try {
				while (regions.hasNext()) {
					SimpleFeature regionOfInterest = regions.next();
					//Do an intersection of parcels with service areas
					intersectingFeatures = intersection(parcels, regionOfInterest);

					//Do a point in polygon intersection parcel/service with residential points
					pipFeatures = pipIntersection(residentialPoints, intersectingFeatures);

					//Dissolve parcel/residential intersection
					SimpleFeature dissolvedResidential = dissolve(pipFeatures, regionOfInterest);
					//Calculate total residential area
					double residentialAreaHectares = ((Geometry)dissolvedResidential.getDefaultGeometry()).getArea()/10000;//getTotalArea(pipFeatures)/10000;
					//double parcelArea = getTotalArea(intersectingFeatures);
					int pipCount = pipCount(residentialPoints, pipFeatures);
					double density = (pipCount / residentialAreaHectares);

					//System.out.println("Total Parcels " + intersectingFeatures.size() + " Res Parcels " + pipFeatures.size());
					//System.out.println("RESAREAHA Area " + regionOfInterest.getAttribute("RESAREAHA") + " Res Area " + residentialAreaHectares);
					//System.out.println("Density: " + density);
					List<String> outputAttrs = new ArrayList();
					outputAttrs.add("NettDensity");
					outputAttrs.add("AreaResidentialHA");
					outputAttrs.add("PointsCountResidential");
					SimpleFeature densityFeature = buildFeature(dissolvedResidential,outputAttrs);
					densityFeature.setAttribute("NettDensity", density);
					densityFeature.setAttribute("AreaResidentialHA", residentialAreaHectares);
					densityFeature.setAttribute("PointsCountResidential", pipCount);
					densityFeatures.add(densityFeature);
				}
				//System.out.print("Processing Complete...");
				resultsSource = DataUtilities.source(DataUtilities.collection(densityFeatures));
				//System.out.println("Found features" + densityFeatures.size());

			} catch (Exception e) {
				LOGGER.error("Failed to complete process for all features");
				//e.printStackTrace();
			} finally {
				regions.close();
			}

		} catch (IOException e) {
			LOGGER.error("Failed to read input/s");
		}
	}

	 private static SimpleFeature buildFeature(SimpleFeature region, List<String> attributes) {

	        SimpleFeatureType sft = (SimpleFeatureType) region.getType();
	        SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
	        stb.init(sft);
	        stb.setName("densityFeatureType");
	        for (String attr : attributes) {
	            stb.add(attr, Double.class);
	        }
	        SimpleFeatureType statsFT = stb.buildFeatureType();
	        SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(statsFT);
	        sfb.addAll(region.getAttributes());

	        return sfb.buildFeature(region.getID());

	    }

	private SimpleFeature dissolve(SimpleFeatureCollection collection, SimpleFeature parent) throws IOException {
		FeatureIterator<SimpleFeature> features = collection.features();
		try {
			List<Geometry> geometries = new ArrayList();
			SimpleFeature feature = null;
			while (features.hasNext()) {
				feature = features.next();
				geometries.add((Geometry) feature.getDefaultGeometry());
			}
			Geometry dissolved = union(geometries);
			SimpleFeature dissolvedFeature = buildFeature(parent,new ArrayList());
			dissolvedFeature.setDefaultGeometry(dissolved);
			return dissolvedFeature;
		} finally {
			features.close();
		}

	}
	
//	private Double getTotalArea(SimpleFeatureCollection features) {
//		double area = 0.0;
//		SimpleFeatureIterator iter = features.features();
//		List<Geometry> geometries = new ArrayList();
//		while (iter.hasNext()) {
//			SimpleFeature feature = iter.next();
//			geometries.add((Geometry) (feature.getDefaultGeometry()));
//			//area += ((Geometry) (feature.getDefaultGeometry())).getArea();
//			//System.out.println("area: " + area);
//		}
//		return union(geometries).getArea();
//		//return area;
//	}

	private Geometry union(List geometries) {
//		double t1 = new Date().getTime();
		Geometry[] geom = new Geometry[geometries.size()];
		geometries.toArray(geom);
		GeometryFactory fact = geom[0].getFactory();
		//PrecisionModel precision = new PrecisionModel(100); // FIXME: should be configurable
		//GeometryFactory fact = new GeometryFactory(precision);
		Geometry geomColl = fact.createGeometryCollection(geom);
		Geometry union = geomColl.union(); //geomColl.buffer(0.0);
//		double t2 = new Date().getTime();
//		LOGGER.info("Time taken Union: " + (t2 - t1) / 1000);
		return union;
	}

//	private static SimpleFeature buildFeatureFromGeometry(SimpleFeatureType featureType, Geometry geom, String id) {
//
//		SimpleFeatureTypeBuilder stb = new SimpleFeatureTypeBuilder();
//		stb.init(featureType);
//		SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(featureType);
//		sfb.add(geom);
//		sfb.add(id);
//
//		return sfb.buildFeature(id);
//	}

	private int pipCount(SimpleFeatureSource points, SimpleFeatureCollection regions) throws IOException {
		int pipCount = 0;
		SimpleFeatureIterator regionsIter = regions.features();

		try {
			while (regionsIter.hasNext()) {
				SimpleFeature region = regionsIter.next();
				pipCount += (intersection(points, region)).size();
			}
			return pipCount;
		} finally {
			regionsIter.close();
		}
	}

	private SimpleFeatureCollection pipIntersection(SimpleFeatureSource points, SimpleFeatureCollection regions) throws IOException {
		SimpleFeatureIterator regionsIter = regions.features();
		SimpleFeatureCollection pipFeatures = DataUtilities.collection(new SimpleFeature[0]);
		try {
			while (regionsIter.hasNext()) {
				SimpleFeature region = regionsIter.next();
				if ((intersection(points, region)).size() > 0){
					pipFeatures.add(region);
				}
			}
			return pipFeatures;
		} finally {
			regionsIter.close();
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
