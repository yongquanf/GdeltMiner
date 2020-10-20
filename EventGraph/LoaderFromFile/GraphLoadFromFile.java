package EventGraph.LoaderFromFile;

import java.io.IOException;

import org.graphstream.graph.ElementNotFoundException;
import org.graphstream.graph.Graph;
import org.graphstream.stream.GraphParseException;

import EventGraph.GraphBuilder.graphStreamIngest;

public class GraphLoadFromFile {

	
	
	/**
	 * load graph from file
	 * @param graphFile
	 * @return
	 */
	public static Graph loadFromGraph(String graphFile) {
		Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
		try {
			graph.read(graphFile);
			return graph;
		} catch (ElementNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GraphParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
}
