package EventGraph.GraphBuilder;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

import com.google.common.collect.Maps;
import util.Util.TripleGP;
import util.models.CAMEOFile;

//import util.tensor.Jama.Matrix;
import util.Util.smile.math.matrix.Matrix;


public class graphStreamIngest {

	/**
	 * default, multigraph
	 */
	public static int graphType = 2;
	/**
	 * 1, single, 2, multple
	 * @param selector
	 * @return
	 */
	public static MultiGraph getGraph(int selector) {
		
//		Graph graph = null;
//		if(selector==1) {
//			graph = new SingleGraph("GraphStream");
//		}else if(selector ==2) {
			//graph = new MultiGraph("MultiStream");
		//}
		
		
		
		return new MultiGraph("MultiStream".concat(Integer.toString(selector)));
	}
	
	/**
	 * event transaction graph
	 * event-node, bipartite graph
	 */
	public void buildTransactionGraph( Graph graph,List<ArrayList<Integer>> transactions) {
		
		Map<Integer,String> eventMap = Maps.newHashMap();
		Map<Integer,String> nodeMap = Maps.newHashMap();
		for(ArrayList<Integer> e: transactions) {
			int eid = e.get(0);
			if(!eventMap.containsKey(eid)) {
				eventMap.put(eid, "Event"+eid);
			}
			//first
			 int node = e.get(1);
			 if(!nodeMap.containsKey(node)) {
				 nodeMap.put(node, "Actor"+node);
			 }
			 //next
			 node = e.get(2);
			 if(!nodeMap.containsKey(node)) {
				 nodeMap.put(node, "Actor"+node);
			 }
		}
		//add event node
		for(Entry<Integer, String> e:eventMap.entrySet()) {
			graph.addNode(e.getValue());
		}
		
		for(Entry<Integer, String> e: nodeMap.entrySet()) {
			graph.addNode(e.getValue());
		}
		
		Iterator<ArrayList<Integer>> ier = transactions.iterator();
		while(ier.hasNext()) {
			ArrayList<Integer> entry = ier.next();
			
			String EventID = "Event"+entry.get(0);
			String actor1 = "Actor"+entry.get(1);
			String actor2 = "Actor"+entry.get(2);
			
			//eventid, from, to
			//node, event,
			
			Node node0 = graph.getNode(EventID);
			//node,from
			Node node1 = graph.getNode(actor1);
			//node,to
			Node node2 = graph.getNode(actor2);
			
			
			//edge:
			if(!node1.hasEdgeBetween(node0)) {
			//from-event
				graph.addEdge(actor1.concat(EventID), node1, node0).setAttribute("length", 1);
			}
			if(!node0.hasEdgeBetween(node2)) {
			//event-to
				graph.addEdge(EventID.concat(actor2),node0,node2).setAttribute("length", 1);
			}
		}
	}
	
	/**
	 * adjacency to graph
	 * @param graph
	 * @param adjacency
	 */
	public void buildTransactionGraph( Graph graph,Matrix adjacencyMatrix) {
		
		//double[][] array =  adjacencyMatrix..getArray();
		
		int r= adjacencyMatrix.nrows();//.getRowDimension();
		int c = adjacencyMatrix.ncols();//.getColumnDimension();
		
		for(int rIdx=0;rIdx<r;rIdx++) {
			String actor1 = "Actor"+rIdx;
			graph.addNode(actor1);
		}
		
		int index = 0;
		for(int rIdx=0;rIdx<r;rIdx++) {
			String actor1 = "Actor"+rIdx;
			
			//double[] rowColumn = array[rIdx];
			
			for(int cIdx=0;cIdx<c;cIdx++) {
				
				if(adjacencyMatrix.get(rIdx,cIdx)>0) {
					
					String actor2 = "Actor"+cIdx;
					
					Node node1= graph.getNode(actor1);
					Node node2=graph.getNode(actor2);
					
					if(!node1.hasEdgeBetween(node2)) {
						Edge e =graph.addEdge(actor1.concat(actor2),node1,node2);	
						e.setAttribute("length", 1);
						//System.out.println("edge: "+(index++));
					}
				}
			}
		}
				
	}
	
	/**
	 * event-event graph
	 * event-event pairwise graph
	 * @param EventSimilaritys
	 */
	public void buildEventSimilarityGraph(Graph graph,List<TripleGP<Integer,Integer,Float>>  EventSimilaritys, double threshold) {
		
		 
	 Iterator<TripleGP<Integer, Integer, Float>> ier = EventSimilaritys.iterator();
		while(ier.hasNext()) {
			 TripleGP<Integer, Integer, Float> item = ier.next();
			//event,event,similarity
			if(item.third<threshold) {
				System.out.println("Not large enough");
				continue;
			}else {
				//event,event,edge
				String id1 = "Event"+item.first;
				String id2 = "Event"+item.second;
				
				Node returnAdded1 = graph.addNode(id1);
				Node returnAdded2 = graph.addNode(id2);
				
				if(!returnAdded1.hasEdgeBetween(returnAdded2)) {
				
					graph.addEdge(id1.concat(id2),returnAdded1,returnAdded2).setAttribute("length", 1);
				}
			}
		}
		System.out.println("Graph: "+graph.toString());
	}

	/**
	 * 
	 * @param graph
	 * @param adjacencyMatrix: adjacency matrix
	 * @param polartoCart: coordinate of each node
	 */
	public void buildTransactionGraph0(Graph graph, 
			Matrix adjacencyMatrix, Matrix polartoCart) {
		// TODO Auto-generated method stub
		
		int r= adjacencyMatrix.nrows();//.getRowDimension();
		int c = adjacencyMatrix.ncols();//.getColumnDimension();
		
		for(int rIdx=0;rIdx<r;rIdx++) {
			String actor1 = "Actor"+rIdx;
			Node one= graph.addNode(actor1);
			one.setAttribute("ui.label",actor1);
			//coordinate
			one.setAttribute("xyz", new double[] { polartoCart.get(rIdx,0), polartoCart.get(rIdx,1), 0 });
		}
		
		int index = 0;
		for(int rIdx=0;rIdx<r;rIdx++) {
			String actor1 = "Actor"+rIdx;
			
			//double[] rowColumn = array[rIdx];
			
			for(int cIdx=0;cIdx<c;cIdx++) {
				
				if(adjacencyMatrix.get(rIdx,cIdx)>0) {
					
					String actor2 = "Actor"+cIdx;
					
					Node node1= graph.getNode(actor1);
					Node node2=graph.getNode(actor2);
					
					if(node1.hasEdgeBetween(node2)) {
						//System.out.println("edge existed!");
					}else {
						Edge e =graph.addEdge(actor1.concat(actor2),node1,node2);	
						//e.setAttribute("length", 1);
						//System.out.println("edge: "+(index++));
					}
				}
			}
		}
		
	}
	
	
	
	/**
	 * write the property to the graph
	 * @param graph
	 * @param adjacencyIdx2ID4Graph
	 * @param adjacencyMatrix
	 * @param polartoCart
	 * @param polarCoord 
	 */
	public void AssignHyperbolicCoords2Graph(Graph graph, 
			Map<String,Integer> ID2adjacencyIdx4Graph,
			Matrix adjacencyMatrix, 
			Matrix polartoCart, Matrix polarCoord) {
		// TODO Auto-generated method stub
		
		
		 Iterator<Node> ier = graph.iterator();
		while(ier.hasNext()) {			
			Node node = ier.next();
			//cartesian
			Map<String,Double> pos = Maps.newHashMap();
			pos.put("x", polartoCart.get(ID2adjacencyIdx4Graph.get(node.getId()),0));
			pos.put("y", polartoCart.get(ID2adjacencyIdx4Graph.get(node.getId()),1));			
			node.setAttribute("vis", pos);
			//polar
			Map<String,Double> polar = Maps.newHashMap();
			polar.put("x", polarCoord.get(ID2adjacencyIdx4Graph.get(node.getId()),0));
			polar.put("y", polarCoord.get(ID2adjacencyIdx4Graph.get(node.getId()),1));			
			
			node.setAttribute("Hyperbolic", polar);
			
		}
		
	}
	
	
	/**
	 * build the graph
	 * @param graph
	 * @param items
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public void buildTransactionGraphEventActorAll(Graph graph, List<ArrayList<Integer>> transactions,			
			Map<Integer, String> indexNodeTable,
			Map<Integer, String> indexActionTable, 
			Map<Integer, Date> indexDateTable) {
		// TODO Auto-generated method stub

		//Map<Integer,String> eventMap = Maps.newHashMap();
		//Map<Integer,String> nodeMap = Maps.newHashMap();
		for(ArrayList<Integer> e: transactions) {
			//event, node, node, action, start
			//String eventID = "Event".concat(Integer.toString(e.get(0)));
			String nodeID1 = "Actor".concat(Integer.toString(e.get(1)));
			String nodeID2 = "Actor".concat(Integer.toString(e.get(2)));
			//label
			String actionCode = indexActionTable.get(e.get(3));
			//number of reported
			int reported = e.get(5);
			
			String actionName ="";
			String code2String = CAMEOFile.getTileFromId(actionCode);
			if(code2String!=null&&!code2String.isEmpty()) {
				actionName = code2String;
			}else {
				actionName= actionCode;
			}
			Instant dateNow = indexDateTable.get(e.get(4)).toInstant();
			String date = dateNow.toString();
			
			//String eventIDLabel1 = actionName;
					
			String nodeID1Label = indexNodeTable.get(e.get(1));
			String nodeID2Label = indexNodeTable.get(e.get(2));
			
			String node0ID = actionName;
			//event
			if(graph.getNode(node0ID)==null) {
				Node node = graph.addNode(node0ID);
				node.setAttribute("label", node0ID);
				node.setAttribute("start", date);
				node.setAttribute("hotness", reported);
			}else {
				Node node = graph.getNode(node0ID);
				int currentLen=(int) Math.round(node.getNumber("hotness"));
				node.setAttribute("hotness", currentLen+reported);
			}
			
			
			if(graph.getNode(nodeID1)==null) {
				Node node = graph.addNode(nodeID1);
				node.setAttribute("label", nodeID1Label);
				node.setAttribute("start", date);
				node.setAttribute("hotness", reported);
				
			}else {
				Node node = graph.getNode(nodeID1);
				int currentLen=(int) Math.round(node.getNumber("hotness"));
				node.setAttribute("hotness", currentLen+reported);
			}
			
			
			if(graph.getNode(nodeID2)==null) {
				Node node = graph.addNode(nodeID2);
				node.setAttribute("label", nodeID2Label);
				node.setAttribute("start", date);
				node.setAttribute("hotness", reported);
			}else {
				Node node = graph.getNode(nodeID2);
				int currentLen=(int) Math.round(node.getNumber("hotness"));
				node.setAttribute("hotness", currentLen+reported);
			}
			
			Node node0 = graph.getNode(node0ID);
			//node,from
			Node node1 = graph.getNode(nodeID1);
			//node,to
			Node node2 = graph.getNode(nodeID2);
			
			 
			
			String edgeID = nodeID1.concat(node0ID).concat(nodeID2);
			//edge:
			if(graph.getEdge(edgeID)==null) {
			//from-event
				Edge edge = graph.addEdge(edgeID, node1, node0,true);
				
				edge.setAttribute("start", date);
				edge.setAttribute("end", date);
				//Set<String> dates = Sets.newHashSet();
				//dates.add(date);
				edge.setAttribute("time",date);
				edge.setAttribute("Types", actionName);
				
				//labrl
				edge.setAttribute("label", nodeID1Label.concat(":").concat(nodeID2Label));
				//length
				edge.setAttribute("length", reported);
			}else {
				
				Edge edge = graph.getEdge(edgeID);
				
				//record the number of events
				if(!edge.hasAttribute("length")) {
					edge.setAttribute("length", 1);
				}else {
					int currentLen=(int) Math.round(edge.getNumber("length"));
					
					
					edge.setAttribute("length", currentLen+reported);
				}
				//has edge
				String date0 = (String)edge.getAttribute("start");
				Instant dateBefore = Instant.parse(date0);
				if(!dateBefore.isBefore(dateNow)) {
					//move back
					edge.setAttribute("start", date);
				}
				//end
				String date1 = (String)edge.getAttribute("end");
				Instant dateLast = Instant.parse(date1);
				if(dateLast.isBefore(dateNow)) {
					edge.setAttribute("end",date);
				}
				
				
			}
			
			edgeID = node0ID.concat(nodeID2).concat(nodeID1);
			if(graph.getEdge(edgeID)==null) {
			//event-to
				Edge edge =graph.addEdge(edgeID,node0,node2,true);
				
				edge.setAttribute("start", date);
				edge.setAttribute("end", date);
				//Set<String> dates = Sets.newHashSet();
				//dates.add(date);
				edge.setAttribute("time",date);
				edge.setAttribute("Types", actionName);
				
				//labrl
				edge.setAttribute("label", nodeID1Label.concat(":").concat(nodeID2Label));
				edge.setAttribute("length", 1);
			}else {
				
				Edge edge = graph.getEdge(edgeID);
				
				//record the number of events
				if(!edge.hasAttribute("length")) {
					edge.setAttribute("length", 1);
				}else {
					int currentLen=(int) Math.round(edge.getNumber("length"));
					edge.setAttribute("length", currentLen+1);
				}
				//has edge
				String date0 = (String)edge.getAttribute("start");
				Instant dateBefore = Instant.parse(date0);
				if(!dateBefore.isBefore(dateNow)) {
					//move back
					edge.setAttribute("start", date);
				}
				//end
				String date1 = (String)edge.getAttribute("end");
				Instant dateLast = Instant.parse(date1);
				if(dateLast.isBefore(dateNow)) {
					edge.setAttribute("end",date);
				}
				
				
			}
		}
	}

	/**
	 * actor graph
	 * @param graph
	 * @param transactions
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public void buildActorTransactionGraph(Graph graph, 
			List<ArrayList<Integer>> transactions,			
			Map<Integer, String> indexNodeTable,
			Map<Integer, String> indexActionTable, 
			Map<Integer, Date> indexDateTable) {
		// TODO Auto-generated method stub

		//allow for multiple edges
		//MultiGraph graph = (MultiGraph)(graph0);
		
		//Map<Integer,String> eventMap = Maps.newHashMap();
		//Map<Integer,String> nodeMap = Maps.newHashMap();
		for(ArrayList<Integer> e: transactions) {
			//event, node, node, action, start
			//String eventID0 = "Event".concat(Integer.toString(e.get(0)));
			String nodeID1 = "Actor".concat(Integer.toString(e.get(1)));
			String nodeID2 = "Actor".concat(Integer.toString(e.get(2)));
			//label
			String actionCode = indexActionTable.get(e.get(3));
			
			//number of mensions
			int reported = e.get(5);
			
			String actionName ="";
			String code2String = CAMEOFile.getTileFromId(actionCode);
			if(code2String!=null&&!code2String.isEmpty()) {
				actionName = code2String;
			}else {
				actionName= actionCode;
			}
			
			Instant dateNow = indexDateTable.get(e.get(4)).toInstant();
			
			String date = dateNow.toString();
			
			//String eventIDLabel1 = actionName;
					
			String nodeID1Label = indexNodeTable.get(e.get(1));
			String nodeID2Label = indexNodeTable.get(e.get(2));
			//event
//			if(graph.getNode(eventID)==null) {
//				Node node = graph.addNode(eventID);
//				node.setAttribute("label", eventIDLabel1);
//				node.setAttribute("start", date);
//			}
			
			if(graph.getNode(nodeID1)==null) {
				Node node = graph.addNode(nodeID1);
				node.setAttribute("label", nodeID1Label);
				//node.setAttribute("start", date);
				node.setAttribute("hotness", reported);
			}else {
				Node node = graph.getNode(nodeID1);
				int currentLen=(int) Math.round(node.getNumber("hotness"));
				node.setAttribute("hotness", currentLen+reported);
			}
			
			
			if(graph.getNode(nodeID2)==null) {
				Node node = graph.addNode(nodeID2);
				node.setAttribute("label", nodeID2Label);
				//node.setAttribute("start", date);
				node.setAttribute("hotness", reported);
			}else {
				Node node = graph.getNode(nodeID1);
				int currentLen=(int) Math.round(node.getNumber("hotness"));
				node.setAttribute("hotness", currentLen+reported);
			}
			
			//Node node0 = graph.getNode(eventID);
			//node,from
			Node node1 = graph.getNode(nodeID1);
			//node,to
			Node node2 = graph.getNode(nodeID2);
			
			//edge:
//			if(!node1.hasEdgeBetween(node2)) {
//			//from-event
//				Edge oneEdge=graph.addEdge(eventIDLabel1, node1, node2);
//				//oneEdge.setAttribute("label", eventIDLabel1);
//				oneEdge.setAttribute("length", 1);
//				oneEdge.setAttribute("start", date);
//				Set<String> dates = Sets.newHashSet();
//				dates.add(date);
//				oneEdge.setAttribute("time",dates);
//				
//			}else {
				//aggregate event
				 String eventID = nodeID1.concat(actionName).concat(nodeID2);
				 
				 
				//add new edge
				if(graph.getEdge(eventID)!=null) {
					//retrieve the edge
					Edge edge = graph.getEdge(eventID);
					
					//update label
					if(!edge.hasAttribute("label")) {
						edge.setAttribute("label", actionName);
					}

					//record the number of events
					if(!edge.hasAttribute("length")) {
						edge.setAttribute("length", reported);
					}else {
						//double currentLen=edge.getNumber("length");
						
						int currentLen=(int) Math.round(edge.getNumber("length"));
						
						edge.setAttribute("length", currentLen+reported);
					}
													
					//save date
					//String dates = (String) edge.getAttribute("time");
					//dates.concat(",").concat(date);
					//edge.setAttribute("time",dates);
					
					//change start date
					String date0 = (String)edge.getAttribute("start");
					Instant dateBefore = Instant.parse(date0);
					if(!dateBefore.isBefore(dateNow)) {
						//move back
						edge.setAttribute("start", date);
					}
					//end
					String date1 = (String)edge.getAttribute("end");
					Instant dateLast = Instant.parse(date1);
					if(dateLast.isBefore(dateNow)) {
						edge.setAttribute("end",date);
					}
					

					
					//save type
					String types = (String) edge.getAttribute("Types");
					types.concat(",").concat(actionName);
					edge.setAttribute("Types", types);
					
				}else {
					Edge oneEdge=graph.addEdge(eventID, node1, node2, true);
					oneEdge.setAttribute("label", actionName);
					
					
					oneEdge.setAttribute("start", date);
					oneEdge.setAttribute("end", date);
					//Set<String> dates = Sets.newHashSet();
					//dates.add(date);
					oneEdge.setAttribute("time",date);
					oneEdge.setAttribute("Types", actionName);
					
					//num of reported
					//record the number of events
					if(!oneEdge.hasAttribute("length")) {
						oneEdge.setAttribute("length", reported);
					}else {
						//double currentLen=edge.getNumber("length");
						
						int currentLen=(int) Math.round(oneEdge.getNumber("length"));
						
						oneEdge.setAttribute("length", currentLen+reported);
					}
					
					/*
					if(!oneEdge.hasAttribute("length")) {
						oneEdge.setAttribute("length", 1);
					}else {
						double currentLen=oneEdge.getNumber("length");
						oneEdge.setAttribute("length", currentLen+1);
					}*/
					
				}
				
				
				
			//}
//			if(!node0.hasEdgeBetween(node2)) {
//			//event-to
//				graph.addEdge(node0.getId().concat(node2.getId()),node0,node2).
//				setAttribute("length", 1);
//			}
		}
	}

	
	
	/**
	 * build the similarity
	 * @param graph
	 * @param items
	 * @param threshold
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public void buildEventSimilarityGraph(Graph graph, Map<Integer,List<Integer>> EventLabels,
			List<TripleGP<Integer, Integer, Float>> EventSimilaritys, float threshold,
			Map<Integer, String> indexNodeTable,
			Map<Integer, String> indexActionTable, 
			Map<Integer, Date> indexDateTable) {
		// TODO Auto-generated method stub
		
		 
		 Iterator<TripleGP<Integer, Integer, Float>> ier = EventSimilaritys.iterator();
			while(ier.hasNext()) {
				 TripleGP<Integer, Integer, Float> item = ier.next();
				//event,event,similarity
				if(item.third<threshold) {
					System.out.println("Not large enough");
					continue;
				}else {
					//event,event,edge
					String id1 = "Event".concat(Integer.toString(item.first));
					String id2 = "Event".concat(Integer.toString(item.second));
									
					
					if(graph.getNode(id1)==null) {
						Node node = graph.addNode(id1);
						//retrieve
						List<Integer> e = EventLabels.get(item.first);
						//action code
						String actionCode = indexActionTable.get(e.get(3));
						
						String actionName ="";
						String code2String = CAMEOFile.getTileFromId(actionCode);
						if(code2String!=null&&!code2String.isEmpty()) {
							actionName = code2String;
						}else {
							actionName= actionCode;
						}
						
						String date = indexDateTable.get(e.get(4)).toInstant().toString();
						
						node.setAttribute("label", actionName);
						node.setAttribute("start", date);
					}
					
					if(graph.getNode(id2)==null) {
						Node node = graph.addNode(id2);
						//retrieve
						List<Integer> e = EventLabels.get(item.second);
						
						//action code
						String actionCode = indexActionTable.get(e.get(3));
						
						String actionName ="";
						String code2String = CAMEOFile.getTileFromId(actionCode);
						if(code2String!=null&&!code2String.isEmpty()) {
							actionName = code2String;
						}else {
							actionName= actionCode;
						}
						
						String date = indexDateTable.get(e.get(4)).toInstant().toString();
						
						node.setAttribute("label", actionName);
						node.setAttribute("start", date);
					}
					
					
					Node returnAdded1 = graph.getNode(id1);
					Node returnAdded2 =graph.getNode(id2);
					
					if(!returnAdded1.hasEdgeBetween(returnAdded2)) {
					
						graph.addEdge(id1.concat(id2),returnAdded1,returnAdded2).
						setAttribute("length",item.third);
					}
				}
			}
			System.out.println("Graph: "+graph.toString());
	}

	public static Graph getGraph(int selector, String transFile) {
		// TODO Auto-generated method stub
		return new MultiGraph("MultiStream".concat(transFile).
				concat(Integer.toString(selector)));
	}
			
}
