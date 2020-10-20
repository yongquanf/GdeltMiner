package EventGraph.GraphBuilder.Gremlin.Gephi;

import org.gephi.graph.api.DirectedGraph;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.Node;
import org.gephi.graph.api.NodeData;
import org.gephi.graph.api.NodeIterator;
import org.gephi.layout.plugin.force.StepDisplacement;
import org.gephi.layout.plugin.force.yifanHu.YifanHuLayout;
import org.openide.util.Lookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphLayout {
	
	private static final Logger logger = LoggerFactory.getLogger(GraphLayout.class);

	/**
	 * graph layout
	 * @return
	 */
	public boolean hyfLayout(){
		//See if graph is well imported
        GraphModel graphModel = Lookup.getDefault().
        		lookup(GraphController.class).getModel();//getGraphModel();
        DirectedGraph graph = graphModel.getDirectedGraph();
        
        System.out.println("Nodes: " + graph.getNodeCount());
        System.out.println("Edges: " + graph.getEdgeCount());
        
        NodeIterator ier = graph.getNodes().iterator();
        	//iterate
      		while(ier.hasNext()){
      			Node one = ier.next();
      			 NodeData n = one.getNodeData();
      			int degree = graph.getDegree(one);
      			n.setSize(20f+degree*0.1f);      			
      		}
        
        
        if(graph.getNodeCount() == 0 && 
        		graph.getEdgeCount() == 0){
        	logger.info("container has no nodes ,no edges");
        	return false;
        }
        
        YifanHuLayout layout = new YifanHuLayout(null, new StepDisplacement(30f));
        layout.setGraphModel(graphModel);
        layout.resetPropertiesValues();
        layout.setOptimalDistance(600f);

        layout.initAlgo();
//        layout.goAlgo();
        for (int i = 0; i < 20 && layout.canAlgo(); i++) {
            layout.goAlgo();
        }
        layout.endAlgo();
        
        return true;
	}
	
}
