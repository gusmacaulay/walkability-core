package org.mccaughey.utilities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;

import com.vividsolutions.jts.geom.Geometry;


public final class GeotoolsAssert {
	public static void assertEquals(SimpleFeatureSource sourceA, SimpleFeatureSource sourceB) throws IOException {
		SimpleFeatureIterator iteratorA = sourceA.getFeatures().features();
		SimpleFeatureIterator iteratorB = sourceB.getFeatures().features();
		//assertEquals(sourceA.getSchema(),(sourceB.getSchema()));
		Map<String, Geometry> mapA = new HashMap();
		try {
			while (iteratorA.hasNext()) {
				SimpleFeature feature = iteratorA.next();
				mapA.put(feature.getID(), (Geometry) feature.getDefaultGeometry());
			}
			while (iteratorB.hasNext()) {
				SimpleFeature feature = iteratorB.next();
				Geometry geom = (Geometry) (feature.getDefaultGeometry());
				Long areaA = Math.round(geom.getArea()/1000); //TODO: improve accuracy
				Long areaB = Math.round(mapA.get(feature.getID()).getArea()/1000);
				Assert.assertEquals(areaA,areaB);
				System.out.println("Area A: " + areaA + " Area B: " + areaB);
			}
		} finally {
			iteratorA.close();
			iteratorB.close();
		}
	}
}
