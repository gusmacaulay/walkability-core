package org.mccaughey.priorityAllocation;

import java.util.List;

import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;

import org.geotools.data.simple.SimpleFeatureSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PointInPolygonPriorityAllocationOMS {
	static final Logger LOGGER = LoggerFactory.getLogger(PointInPolygonPriorityAllocationOMS.class);

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
	 * The priority list of which land uses, to figure out which one to allocate
	 */
	@In
	public List<String> landUsePriorityList;

	/**
	 * The resulting parcels with re-allocated land use types
	 */
	@Out
	public SimpleFeatureSource resultParcels;

	/**
	 * Reads in the population count layer and regions layer from given URLs, writes out average density results to
	 * resultsURL
	 */
	@Execute
	public void allocate() {
		resultParcels = parcels;
	}
}
