package org.mccaughey.density;

import java.io.IOException;
import java.util.ArrayList;
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
import org.geotools.graph.structure.Graph;
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
			SimpleFeatureCollection intersectingFeatures = DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection dissolvedParcels = DataUtilities.collection(new SimpleFeature[0]);
			SimpleFeatureCollection pipFeatures = DataUtilities.collection(new SimpleFeature[0]);
			try {
				while (regions.hasNext()) {
					SimpleFeature regionOfInterest = regions.next();
					//Do an intersection of parcels with service areas
					intersectingFeatures = intersection(parcels, regionOfInterest);

					//Do an point in polygon intersection parcel/service with residential points
					pipFeatures.addAll(pipIntersection(residentialPoints.getFeatures(),DataUtilities.source(intersectingFeatures)));
					//Dissolve parcel/service intersection
					LOGGER.info("Attempting Dissolvv ...");
					dissolvedParcels.add(dissolve(intersectingFeatures, regionOfInterest));
					//Dissolve parcel/residential intersection
					
					//Calculate proportion(density) of parcel/service:parcel/residential
				}
				System.out.print("Processing Complete...");
				resultsSource = DataUtilities.source(pipFeatures);
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
		Geometry[] geom = new Geometry[geometries.size()];
		geometries.toArray(geom);
		GeometryFactory fact = geom[0].getFactory();
		Geometry geomColl = fact.createGeometryCollection(geom);
		Geometry union = geomColl.buffer(0.0);
		return union;
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
			while(pointsIter.hasNext()) {
				SimpleFeature point = pointsIter.next();
				pipFeatures.addAll(intersection(regions,point));
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

		return features.subCollection(filter);

	}
}
