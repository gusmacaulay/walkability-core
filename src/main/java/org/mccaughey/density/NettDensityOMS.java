package org.mccaughey.density;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.geotools.feature.DefaultFeatureCollection;
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

/**
 * Performs Nett Density calculation, which is the ratio of the number of
 * dwellings over area of land utilised as residential land within a
 * participant’s neighbourhood
 * 
 * @author amacaulay
 * 
 */
public class NettDensityOMS {
	static final Logger LOGGER = LoggerFactory.getLogger(NettDensityOMS.class);

	/**
	 * The input regions to calculate density for
	 */
	@In
	@Name("Neighbourhoods")
	public SimpleFeatureSource regionsOfInterest;

	/**
	 * A land parcel type data set (eg. cadastre)
	 */
	@In
	@Name("Land parcel type data set")
	@Description("A land parcel type data set (eg. cadastre)")
	public SimpleFeatureSource parcels;

	/**
	 * Residential points data set (for figuring out what is/isn't residential)
	 */
	@In
	@Name("Residential points data set")
	@Description("Residential points data set (for figuring out what is/isn't residential)")
	public SimpleFeatureSource residentialPoints;

	/**
	 * The resulting regions with average density calculated
	 */
	@Out
	@Name("Resulting regions")
	@Description("The resulting regions with average density calculated")
	public SimpleFeatureSource resultsSource;

	/**
	 * Reads in the population count layer and regions layer from given sources,
	 * writes out Nett Density results to resultsSource
	 * @throws IOException 
	 */
	@Execute
	public void nettDensity() throws IOException {
		try {

			FeatureIterator<SimpleFeature> regions = regionsOfInterest
					.getFeatures().features();
			SimpleFeatureCollection intersectingFeatures;
			List<SimpleFeature> densityFeatures = new ArrayList<SimpleFeature>();
			SimpleFeatureCollection pipFeatures;
			LOGGER.info("Calculating Density");
			try {
				while (regions.hasNext()) {

					SimpleFeature regionOfInterest = regions.next();
					// Do an intersection of parcels with service areas
					intersectingFeatures = intersection(parcels,
							(Geometry)regionOfInterest.getDefaultGeometry());

					// Do a point in polygon intersection parcel/service with
					// residential
					// points
					pipFeatures = pipIntersection(residentialPoints,
							intersectingFeatures);

					// Calculate total residential area
					Geometry residentialDissolved = getDissolvedParcel(pipFeatures);
					double residentialAreaHectares = residentialDissolved
							.getArea() / 10000;
					
					// Count residential features in residential area
					int pipCount = pipCount(residentialPoints,
							residentialDissolved);
					double density = (pipCount / residentialAreaHectares);

					List<String> outputAttrs = new ArrayList<String>();
					outputAttrs.add("NettDensity");
					outputAttrs.add("AreaResidentialHA");
					outputAttrs.add("PointsCountResidential");
					SimpleFeature densityFeature = buildFeature(
							regionOfInterest, outputAttrs);
					densityFeature.setAttribute("NettDensity", density);
					densityFeature.setAttribute("AreaResidentialHA",
							residentialAreaHectares);
					densityFeature.setAttribute("PointsCountResidential",
							pipCount);
					densityFeatures.add(densityFeature);
				}
				resultsSource = DataUtilities.source(DataUtilities
						.collection(densityFeatures));
				LOGGER.info("Completed density calculation");

			}  finally {
				regions.close();
			}

		} catch (IOException e) {
			throw new IOException("Failed to read input/s for Nett Density",e);
		}
	}

	private int pipCount(SimpleFeatureSource points,
			Geometry residentialDissolved) throws IOException {

		return (intersection(points, residentialDissolved)).size();

	}

	private SimpleFeatureCollection intersection(SimpleFeatureSource featuresOfInterest,
			Geometry regionOfInterest) throws IOException {
		SimpleFeatureCollection features = featuresOfInterest.getFeatures();
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
		String geometryPropertyName = features.getSchema()
				.getGeometryDescriptor().getLocalName();

		Filter filter = ff.intersects(ff.property(geometryPropertyName),
				ff.literal(regionOfInterest));

		return featuresOfInterest.getFeatures(filter); // --> DO THIS INSTEAD
	}

	private Geometry getDissolvedParcel(SimpleFeatureCollection features) {
		SimpleFeatureIterator iter = features.features();
		List<Geometry> geometries = new ArrayList<Geometry>();
		while (iter.hasNext()) {
			SimpleFeature feature = iter.next();
			geometries.add((Geometry) (feature.getDefaultGeometry()));
		}
		return union(geometries);
	}

	private Double getTotalArea(SimpleFeatureCollection features) {
		double area = 0.0;
		SimpleFeatureIterator iter = features.features();
		List<Geometry> geometries = new ArrayList<Geometry>();
		while (iter.hasNext()) {
			SimpleFeature feature = iter.next();
			geometries.add((Geometry) (feature.getDefaultGeometry()));
		}
		return union(geometries).getArea();
	}

	private static SimpleFeature buildFeature(SimpleFeature region,
			List<String> attributes) {

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

//	private SimpleFeature dissolve(SimpleFeatureCollection collection,
//			SimpleFeature parent) throws IOException {
//		FeatureIterator<SimpleFeature> features = collection.features();
//		try {
//			List<Geometry> geometries = new ArrayList();
//			SimpleFeature feature = null;
//			while (features.hasNext()) {
//				feature = features.next();
//				geometries.add((Geometry) feature.getDefaultGeometry());
//			}
//			Geometry dissolved = union(geometries);
//			SimpleFeature dissolvedFeature = buildFeature(parent,
//					new ArrayList());
//			dissolvedFeature.setDefaultGeometry(dissolved);
//			return dissolvedFeature;
//		} finally {
//			features.close();
//		}
//
//	}

	private Geometry union(List<Geometry> geometries) {
		Geometry[] geom = new Geometry[geometries.size()];
		geometries.toArray(geom);
		GeometryFactory fact = geom[0].getFactory();
		Geometry geomColl = fact.createGeometryCollection(geom);
		Geometry union = geomColl.union();

		return union;
	}

//	private int pipCount(SimpleFeatureSource points,
//			SimpleFeatureCollection regions) throws IOException {
//		int pipCount = 0;
//		SimpleFeatureIterator regionsIter = regions.features();
//
//		try {
//			while (regionsIter.hasNext()) {
//				SimpleFeature region = regionsIter.next();
//				pipCount += (intersection(points, (Geometry)region.getDefaultGeometry())).size();
//			}
//			return pipCount;
//		} finally {
//			regionsIter.close();
//		}
//	}

	private SimpleFeatureCollection pipIntersection(SimpleFeatureSource points,
			SimpleFeatureCollection regions) throws IOException {
		SimpleFeatureIterator regionsIter = regions.features();
		DefaultFeatureCollection pipFeatures = new DefaultFeatureCollection();
		try {
			while (regionsIter.hasNext()) {
				SimpleFeature region = regionsIter.next();
				if ((intersection(points, (Geometry)region.getDefaultGeometry())).size() > 0) {
					pipFeatures.add(region);
				}
			}
			return pipFeatures;
		} finally {
			regionsIter.close();
		}
	}

//	private SimpleFeatureCollection intersection(
//			SimpleFeatureSource featuresOfInterest,
//			SimpleFeature intersectingFeature) throws IOException {
//
//		SimpleFeatureCollection features = featuresOfInterest.getFeatures();
//		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
//		String geometryPropertyName = features.getSchema()
//				.getGeometryDescriptor().getLocalName();
//
//		Filter filter = ff.intersects(ff.property(geometryPropertyName),
//				ff.literal(intersectingFeature.getDefaultGeometry()));
//
//		// return features.subCollection(filter); --> THIS IS REALLY SLOW
//		return featuresOfInterest.getFeatures(filter); // --> DO THIS INSTEAD
//
//	}
}
