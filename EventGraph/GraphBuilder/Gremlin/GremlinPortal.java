package EventGraph.GraphBuilder.Gremlin;



import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;

/**
 * graph stream, in graphML format, write to gremlin,
 * write gexf to gremlin
 * gremlin provide universal query to application
 * 
 * @author eric
 *
 */
public class GremlinPortal {
	
	private TinkerGraph tg;
	  private GraphTraversalSource g;
	  

	  // Try to create a new graph and load the specified GraphML file
	  public boolean loadGraphInGraphML(String name)
	  {
	    tg = TinkerGraph.open() ;
	    
	    try
	    {
	      tg.io(IoCore.graphml()).readGraph(name);
	    }
	    catch( IOException e )
	    {
	      System.out.println("GraphStats - GraphML file not found");
	      return false;
	    }
	    g = tg.traversal();
	    return true;
	  }
	  
	  /**
	   * basic test
	   * @param g
	   */
	  public void basicGraphStatistics(GraphTraversalSource g) {
		  
		    // Display some basic demographics
		    // Note that label has to be prefixed by "T."
		    Map verts = g.V().groupCount().by(T.label).next();
		    Map edges = g.E().groupCount().by(T.label).next();
		    System.out.println("Vertices : " + verts);
		    System.out.println("Edges    : " + edges);
	  }
	  
	  /**
	   * remote connect
	   */
	  public GraphTraversalSource getRemoteGraph(String host, int port) {
		  Cluster.Builder builder = Cluster.build();
		    builder.addContactPoint(host);
		    builder.port(port);
		    builder.serializer(new GryoMessageSerializerV1d0());

		    Cluster cluster = builder.create();
  DriverRemoteConnection cfg = DriverRemoteConnection.using(cluster);
		    GraphTraversalSource g =
		      EmptyGraph.instance().traversal().withRemote(cfg);
		    return g;
	  }
	  
	  
	  
	  
	  // Try to create a new empty graph instance
	  public boolean createGraph()
	  {
	    tg = TinkerGraph.open() ;
	    g = tg.traversal();
	    
	    if (tg == null || g==null)
	    {
	      return false;
	    }
	    return true;
	  }

	  // Add the specified vertices and edge. Do not add anything that 
	  // already exists.
	  public boolean addElements(String name1, String label, String name2)
	  {
	    if (tg == null || g==null)
	    {
	      return false;
	    }

	    // Create a node for 'name1' unless it exists already
	    Vertex v1 = 
	      g.V().has("name",name1).fold().
	            coalesce(__.unfold(),__.addV().property("name",name1)).next();

	    // Create a node for 'name2' unless it exists already
	    Vertex v2 = 
	      g.V().has("name",name2).fold().
	            coalesce(__.unfold(),__.addV().property("name",name2)).next();

	    // Create an edge between 'name1' and 'name2' unless it exists already
	    g.V().has("name",name1).out(label).has("name",name2).fold().
	          coalesce(__.unfold(),
	                   __.addE(label).from(__.V(v1)).to(__.V(v2))).iterate();

	    return true;
	  }

	  public void displayGraph()
	  {
	    Long c;
	    c = g.V().count().next();
	    System.out.println("Graph contains " + c + " vertices");
	    c = g.E().count().next();
	    System.out.println("Graph contains " + c + " edges");

	    List<Path> edges = g.V().outE().inV().path().by("name").by().toList();

	    for (Path p : edges)
	    {
	      System.out.println(p);
	    }
	  }

	  public void parseCSVGraph(String inputFile) {
		  
	        String line;
	        String [] values;

	        try {
	        FileReader fileReader = new FileReader(inputFile);

	        BufferedReader bufferedReader = new BufferedReader(fileReader);

	        while((line = bufferedReader.readLine()) != null) 
	        {
	          //System.out.println(line);
	          values = line.split(",");
	          addElements(values[0],values[1],values[2]);
	        }

	        displayGraph();
	        bufferedReader.close();         
	      }
	      catch( Exception e ) 
	      {
	        System.out.println("Unable to open file" + e.toString());
	        //e.printStackTrace();
	      }
	  }
}
