/*
 * Copyright (C) 2012 amacaulay
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.mccaughey.connectivity;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import oms3.annotations.Description;
import oms3.annotations.Execute;
import oms3.annotations.In;
import oms3.annotations.Out;
import org.apache.commons.io.FileUtils;
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
 *
 * @author amacaulay
 */
@Description("Generates network service areas for points on a network")
public class NetworkBufferOMS {

    static final Logger LOGGER = LoggerFactory.getLogger(NetworkBufferOMS.class);
    /**
     * The road network to count connections from
     */
    @In
    public URL network;
    /**
     * The points of interest
     */
    @In
    public URL points;
    
    /**
     * The network distance for the service areas (maximum walk distance)
     */
    @In
    public Double distance;
    
    /**
     * The buffer size
     */
    @In
    public Double bufferSize;
    /**
     * The resulting regions
     */
    @Out
    public URL regions;

    @Execute
    public void run() throws Exception {
//        try {
            SimpleFeatureSource networkSource = DataUtilities.source(readFeatures(network));
            SimpleFeatureSource pointsSource = DataUtilities.source(readFeatures(points));

            NetworkBufferBatch nbb = new NetworkBufferBatch(networkSource,pointsSource.getFeatures(),distance,bufferSize);
            SimpleFeatureCollection buffers = nbb.createBuffers();
            File file = new File("service_areas_oms.json");
            FileUtils.writeStringToFile(file, writeFeatures(buffers));
            regions = file.toURI().toURL();

//        } catch (Exception e) { //Can't do much here because of OMS?
//            LOGGER.error(e.getMessage());
//        }
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

    private String writeFeatures(SimpleFeatureCollection features) throws IOException {
        FeatureJSON fjson = new FeatureJSON();
        Writer writer = new StringWriter();

        fjson.writeFeatureCollection(features, writer);

        return writer.toString();
    }
}
