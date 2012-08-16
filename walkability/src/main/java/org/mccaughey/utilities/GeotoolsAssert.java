package org.mccaughey.utilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.util.AssertionFailedException;

public final class GeotoolsAssert {

	static final Logger LOGGER = LoggerFactory.getLogger(GeotoolsAssert.class);

	private GeotoolsAssert() {
	}
	
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
			//	System.out.println("Geometry Name: " + featureA.getDefaultGeometryProperty().getName());
				Geometry geomB = (Geometry) (featureB.getDefaultGeometry());
				Geometry geomA = (Geometry) (featureA.getDefaultGeometry());
				Long areaA = Math.round(geomB.getArea() / 1000); //TODO: improve accuracy
				Long areaB = Math.round(geomA.getArea() / 1000);
				if (!areaA.equals(areaB)) {
					throw new AssertionFailedException("Geometry Areas not equal,expected: " + areaA + " but was: " + areaB);
				}
				LOGGER.info("Area A: " + areaA + " Area B: " + areaB);

				//Test properties (excluding geometry) equivalence --> A can be a *subset* of B, comparison is not symmetrical
				for (Property p : featureA.getProperties()) {
					if (p.getName() != featureA.getDefaultGeometryProperty().getName()) {
					//	System.out.println(("Comparing " + p.toString()));
						String valueB = featureB.getProperty(p.getName()).getValue().toString();
						LOGGER.info("A {}, B {}.", p.getValue(), valueB);
						if(!p.getValue().toString().equals(valueB.toString())) {
							throw new AssertionFailedException("Feature Properties not equal, expected:" + p.getValue() + " but got: " + valueB);
						}
					}
				}
			}
		} finally {
			iteratorA.close();
			iteratorB.close();
		}
	}
}
