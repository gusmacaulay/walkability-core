/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mccaughey.connectivity;


import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.util.Collection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.graph.build.feature.FeatureGraphGenerator;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.Node;
import org.opengis.feature.Feature;

/**
 *
 * @author gus
 */
public class ConnectivityIndex {

    public static double connectivity(SimpleFeatureSource featureSource, Geometry roi) throws Exception {
        
        double area = roi.getArea();
        System.out.println("Area:" + String.valueOf(area));
        Graph graph = buildLineNetwork(featureSource, roi);
        System.out.println("Connections:" + String.valueOf(countConnections(graph)));
        return countConnections(graph)/area;
    }

    private static Graph buildLineNetwork(SimpleFeatureSource featureSource, Geometry roi) throws IOException {        
        // get a feature collection
        SimpleFeatureCollection fCollection = featureSource.getFeatures();

        //create a linear graph generate
        LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();

        //wrap it in a feature graph generator
        FeatureGraphGenerator featureGen = new FeatureGraphGenerator(lineStringGen);

        //put all the features that intersect the roi into  the graph generator
        FeatureIterator iter = fCollection.features();
        try {
            while (iter.hasNext()) {
                Feature feature = iter.next();
                if (roi.intersects((Geometry)feature.getDefaultGeometryProperty().getValue())) 
                    featureGen.add(feature);
            }
        } finally {
            iter.close();
        }
        return featureGen.getGraph();
    }
    
    private static int countConnections(Graph graph) {
        int count = 0;
        for (Node node : (Collection<Node>)graph.getNodes()) {
            if (node.getEdges().size() >= 3) { //3 or more legged nodes are connected
                count++;
            }
       }
       return count;
    }
    
}
