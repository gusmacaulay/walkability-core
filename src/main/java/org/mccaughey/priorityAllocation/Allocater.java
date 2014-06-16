package org.mccaughey.priorityAllocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs the priority allocation in a region of interest, according to a priority lookup. This is called by an
 * executor service
 * 
 * @author amacaulay
 * 
 */
class Allocater implements Callable<List<SimpleFeature>> {

  static final Logger LOGGER = LoggerFactory.getLogger(Allocater.class);

  private SimpleFeature regionOfInterest;
  private SimpleFeatureSource pointFeatures;
  private Map<String, Integer> priorityOrder;
  private SimpleFeatureSource parcels;
  private String landUseAttribute;

  Allocater(SimpleFeature regionOfInterest, SimpleFeatureSource pointFeatures, Map<String, Integer> priorityOrder,
      SimpleFeatureSource parcels, String landUseAttribute) {
    this.regionOfInterest = regionOfInterest;
    this.pointFeatures = pointFeatures;
    this.priorityOrder = priorityOrder;
    this.parcels = parcels;
    this.landUseAttribute = landUseAttribute;
  }

  /**
   * Does and intersection of parcels with the region of interest and then allocates each parcel.
   * 
   * @throws CQLException
   */
  public List<SimpleFeature> call() throws IOException, CQLException {
    // Do an intersection of parcels with service areas
    SimpleFeatureCollection intersectingParcels = AllocationUtils.intersection(parcels, regionOfInterest);
    // Priority allocation
    // for each intersecting parcel
    FeatureIterator<SimpleFeature> unAllocatedParcels = intersectingParcels.features();

    List<SimpleFeature> allocatedParcels = new ArrayList<SimpleFeature>();
    while (unAllocatedParcels.hasNext()) {
      SimpleFeature parcel = unAllocatedParcels.next();

      SimpleFeature allocatedParcel = allocateParcel(pointFeatures, parcel, priorityOrder);
      allocatedParcels.add(allocatedParcel);
    }
    unAllocatedParcels.close();

    SimpleFeatureCollection allocatedFeatures = DataUtilities.collection(allocatedParcels);
    SimpleFeatureSource source = DataUtilities.source(AllocationUtils.prioritiseOverlap(allocatedFeatures,
        landUseAttribute, priorityOrder));
    return AllocationUtils.dissolveByCategory(source.getFeatures(), landUseAttribute, priorityOrder.keySet());
  }

  private SimpleFeature allocateParcel(SimpleFeatureSource pointPriorityFeatures, SimpleFeature parcelOfInterest,
      Map<String, Integer> priorityOrder) throws IOException {
    FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    String geometryPropertyName = pointPriorityFeatures.getSchema().getGeometryDescriptor().getLocalName();

    Filter filter = ff.intersects(ff.property(geometryPropertyName), ff.literal(parcelOfInterest.getDefaultGeometry()));

    SimpleFeatureIterator pointFeatures = pointPriorityFeatures.getFeatures(filter).features();
    SimpleFeatureType ft = augmentType("Allocated Parcel",parcelOfInterest.getFeatureType(), landUseAttribute, String.class);
    try {
      int currentPriority = priorityOrder.size() + 1;
      String currentPriorityClass = "";
      while (pointFeatures.hasNext()) {
        SimpleFeature comparisonPoint = pointFeatures.next();

        String landUse = comparisonPoint.getAttribute(landUseAttribute).toString();

        if (priorityOrder.containsKey(landUse)) {
          int comparisonPriority = priorityOrder.get(landUse);
          if (comparisonPriority < currentPriority) {
            currentPriority = comparisonPriority;
            currentPriorityClass = landUse;
          }
        } else {
         // LOGGER.debug("Misssing priority value for classification " + landUse);
        }
      }
      if (currentPriorityClass != "") {
        List<String> priorityValue = new ArrayList<String>();
        priorityValue.add(currentPriorityClass);
        return AllocationUtils.buildFeature(ft, parcelOfInterest, priorityValue);
      } else { // don't add parcels which have no classification of
        // interest
        return null;
      }
    } finally {
      pointFeatures.close();
    }
  }

  private SimpleFeatureType augmentType(String name, SimpleFeatureType featureType, String landUseAttribute2, Class<?> type) {
    SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    builder.setName(name);
    builder.addAll(featureType.getAttributeDescriptors());
    builder.add(landUseAttribute, type);
    builder.setCRS(featureType.getCoordinateReferenceSystem());
    return builder.buildFeatureType();
  }
}
