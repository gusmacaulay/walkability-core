package org.mccaughey.statistics;

import java.io.IOException;
import java.net.URL;

import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;

import org.geotools.factory.CommonFactoryFinder;
import org.mccaughey.utilities.GeoJSONUtilities;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionAverageOMS {
	static final Logger LOGGER = LoggerFactory.getLogger(CollectionAverageOMS.class);

	@In
	URL features;

	@In
	String attribute;

	@Out
	Double result;

	@Execute
	public void average() {
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);
		Function sum = ff.function("Collection_Average", ff.property(attribute));

		try {
			result = (Double) sum.evaluate(GeoJSONUtilities.readFeatures(features));
		} catch (IOException e) {
			LOGGER.error("Failed to read features");
			result = null;
		}
	}

}
