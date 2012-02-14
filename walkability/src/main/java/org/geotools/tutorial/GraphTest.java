/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.tutorial;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.graph.build.feature.FeatureGraphGenerator;
import org.geotools.graph.build.line.LineStringGraphGenerator;
import org.geotools.graph.structure.Edge;
import org.geotools.graph.structure.Graph;
import org.geotools.graph.structure.GraphVisitor;
import org.geotools.graph.structure.Graphable;
import org.geotools.graph.traverse.GraphIterator;
import org.geotools.graph.traverse.GraphTraversal;
import org.geotools.graph.traverse.basic.BasicGraphTraversal;
import org.geotools.graph.traverse.basic.SimpleGraphWalker;
import org.geotools.graph.traverse.standard.BreadthFirstIterator;
import org.opengis.feature.Feature;

/**
 *
 * @author amacaulay
 */
public class GraphTest {

    public static void main(String[] args) throws Exception {
        //Read a shapefile in ...
        SimpleFeatureSource featureSource = openShapeFile("/home/amacaulay/apache-tomcat-7.0.22/webapps/geoserver/data/data/taz_shapes/tasmania_roads.shp");
        //Build a line network graph
        Graph graph = buildLineNetwork(featureSource);
        System.out.println("Features count: " + String.valueOf(featureSource.getFeatures().size()));
        System.out.println("Graph Edges: " + String.valueOf(graph.getEdges().size()));
        //Count all the orphans 
        int orphans = countOrphans(graph);
        System.out.println("Orphan Count: " + String.valueOf(orphans));
    
    }

    private static SimpleFeatureSource openShapeFile(String filename) throws Exception {
        File shapeFile = new File(filename);
        // Connect to the shapefile
        FileDataStore dataStore = FileDataStoreFinder.getDataStore(shapeFile);
        return dataStore.getFeatureSource();
    }

    private static Graph buildLineNetwork(SimpleFeatureSource featureSource) throws IOException {
//        final LineGraphGenerator generator = new BasicLineGraphGenerator();
//        SimpleFeatureCollection fc = featureSource.getFeatures();
//
//        fc.accepts(new FeatureVisitor() {
//
//            public void visit(Feature feature) {
//                generator.add(feature);
//            }
//        }, null);
//        return generator.getGraph();
        
        // get a feature collection somehow
        SimpleFeatureCollection fCollection = featureSource.getFeatures();

        //create a linear graph generate
        LineStringGraphGenerator lineStringGen = new LineStringGraphGenerator();

        //wrap it in a feature graph generator
        FeatureGraphGenerator featureGen = new FeatureGraphGenerator(lineStringGen);

        //throw all the features into the graph generator
        FeatureIterator iter = fCollection.features();
        try {
            while (iter.hasNext()) {
                Feature feature = iter.next();
                featureGen.add(feature);
            }
        } finally {
            iter.close();
        }
        return featureGen.getGraph();
    }
    
    private static int countOrphans(Graph graph) {
        int count = 0;
        for(Edge edge : (Collection<Edge>)graph.getEdges()) {
             Iterator related = edge.getRelated();
                if (related.hasNext() == false) {
                    // no related components makes this an orphan
                    count++;
                }
        }
        return count;
//        class OrphanVisitor implements GraphVisitor {
//
//            private int count = 0;
//
//            public int getCount() {
//                return count;
//            }
//
//            public int visit(Graphable component) {
//                Iterator related = component.getRelated();
//                if (related.hasNext() == true) {
//                    // no related components makes this an orphan
//                    count++;
//                }
//                return GraphTraversal.CONTINUE;
//            }
//        }
//        OrphanVisitor graphVisitor = new OrphanVisitor();
//
//        SimpleGraphWalker sgv = new SimpleGraphWalker(graphVisitor);
//        
//        GraphIterator iterator = new BreadthFirstIterator();
//        BasicGraphTraversal bgt = new BasicGraphTraversal(graph, sgv, iterator);
//
//        bgt.traverse();
//
//        return graphVisitor.getCount();

    }
}
