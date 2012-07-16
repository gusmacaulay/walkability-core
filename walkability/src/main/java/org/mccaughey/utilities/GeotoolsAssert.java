package org.mccaughey.utilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public final class GeotoolsAssert {

	static final Logger LOGGER = LoggerFactory.getLogger(GeotoolsAssert.class);

	public static void assertEquals(SimpleFeatureSource sourceA, SimpleFeatureSource sourceB) throws IOException {
		SimpleFeatureIterator iteratorA = sourceA.getFeatures().features();
		SimpleFeatureIterator iteratorB = sourceB.getFeatures().features();
		//assertEquals(sourceA.getSchema(),(sourceB.getSchema()));
		Map<String, SimpleFeature> mapA = new HashMap();
		try {
			while (iteratorA.hasNext()) {
				SimpleFeature feature = iteratorA.next();
				mapA.put(feature.getID(), feature);
			}
			while (iteratorB.hasNext()) {
				SimpleFeature featureB = iteratorB.next();
				SimpleFeature featureA = mapA.get(featureB.getID());
				//Test Geometry area equivalence
				Geometry geomB = (Geometry) (featureB.getDefaultGeometry());
				Geometry geomA = (Geometry) (featureA.getDefaultGeometry());
				Long areaA = Math.round(geomB.getArea() / 1000); //TODO: improve accuracy
				Long areaB = Math.round(geomA.getArea() / 1000);
				Assert.assertEquals(areaA, areaB);
				LOGGER.info("Area A: " + areaA + " Area B: " + areaB);

				//Test properties (excluding geometry) equivalence --> A can be a *subset* of B, comparison is not symetrical
				for (Property p : featureA.getProperties()) {
					if (p.getName() != featureA.getDefaultGeometryProperty().getName()) {
						System.out.println(("Comparing " + p.toString()));
						LOGGER.info("A {}, B {}.", p.getValue(), featureB.getProperty(p.getName()).getValue());
						Assert.assertEquals(p.getValue().toString(), featureB.getProperty(p.getName().getLocalPart()).getValue().toString());
					}
				}
			}
		} finally {
			iteratorA.close();
			iteratorB.close();
		}
	}
}
