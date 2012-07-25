package org.mccaughey.density;

import java.io.IOException;

import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.mccaughey.spatial.IntersectionOMS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

			//Do an intersection of parcels with service areas
			resultsSource = intersection(parcels, regionsOfInterest);

			//Do an point in polygon intersection parcel/service with residential points

			//Dissolve parcel/service intersection

			//Dissolve parcel/residential intersection

			//Calculate proportion(density) of parcel/service:parcel/residential

			

		} catch (IOException e) {
			LOGGER.error("Failed to read input/s");
		}
	}

	private SimpleFeatureSource intersection(SimpleFeatureSource featuresOfInterest, SimpleFeatureSource regionsOfInterest) {
		try {

			SimpleFeatureCollection features = featuresOfInterest.getFeatures();
			FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
			String geometryPropertyName = features.getSchema().getGeometryDescriptor().getLocalName();
			FeatureIterator<SimpleFeature> regions = regionsOfInterest.getFeatures().features();
			SimpleFeatureCollection intersectingFeatures = DataUtilities.collection(new SimpleFeature[0]);
			while(regions.hasNext()) {
				SimpleFeature regionOfInterest = regions.next();
				Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(regionOfInterest.getDefaultGeometry()));
				intersectingFeatures.addAll(features.subCollection(filter));
			}
			return DataUtilities.source(intersectingFeatures);
		} catch (IOException e) {
			LOGGER.equals("Failed to read input datasets");
		}
		return null;
	}
}
