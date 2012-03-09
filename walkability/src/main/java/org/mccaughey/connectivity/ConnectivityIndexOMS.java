/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import oms3.annotations.*;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper for ConnectivityIndex that provides OMS3 annotations so it
 * can be used in OMS3 workflows, scripts etc.
 *
 * @author amacaulay
 */
@Description("Calculates Connectivity for a given network and region")
public class ConnectivityIndexOMS {

    static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityIndexOMS.class);
    /**
     * The road network to count connections from
     */
    @In
    public URL network;
    /**
     * The region if interest
     */
    @In
    public URL regions;
    /**
     * The resulting connectivity
     */
    @Out
    public String results;

    /**
     * Processes the featureSource network and region to calculate connectivity
     *
     * @throws Exception
     */
    @Execute
    public void run() {
        try {
            SimpleFeatureSource networkSource = DataUtilities.source(readFeatures(network));
            SimpleFeatureSource regionSource = DataUtilities.source(readFeatures(regions));

            ConnectivityIndexFJ cifj = new ConnectivityIndexFJ(networkSource,regionSource.getFeatures());
            cifj.connectivity();
            results = writeFeatures(cifj.results);
            LOGGER.info(results);
//            System.out.println(results);
            
        } catch (Exception e) { //Can't do much here because of OMS?
            LOGGER.error(e.getMessage());
        }
    }

    private String writeFeatures(SimpleFeatureCollection features) throws IOException {
        FeatureJSON fjson = new FeatureJSON();
        Writer writer = new StringWriter();

        fjson.writeFeatureCollection(features, writer);

        return writer.toString();

    }

  

    private SimpleFeatureCollection readFeatures(URL url) throws IOException {
        FeatureJSON io = new FeatureJSON();
      
        String json = readURL(url);
        Reader reader = new StringReader(json);
        
        FeatureIterator<SimpleFeature> features = io.streamFeatureCollection(url.openConnection().getInputStream());
        
    //    Reader reader = GeoJSONUtil.toReader(url.openConnection().getInputStream());
      
        
        io.readFeatureCollection(reader);
        SimpleFeatureCollection collection = FeatureCollections.newCollection();
        SimpleFeature feature;
        
        while (features.hasNext()) {
            feature = (SimpleFeature) features.next();
            collection.add(feature);
        }

        return collection;
    }

    private String readURL(URL url) throws IOException {

        URLConnection uc = url.openConnection();

        InputStreamReader input = new InputStreamReader(uc.getInputStream());
        BufferedReader in = new BufferedReader(input);
        String inputLine;
        StringBuilder sb = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            sb.append(inputLine);
        }

        in.close();
        String json = sb.toString();
       // System.out.print(json);
        return json;
    }
    
}
