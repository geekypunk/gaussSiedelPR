package edu.cornell.lsi.proj2.pagerank.blocked.utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Stack;

public class BlockInfo 
{
	static final int SOURCEINDEXBEGIN = 0;
	static final int SOURCEINDEXEND = 6;
	static final int DESTINDEXBEGIN = 7;
	static final int DESTINDEXEND = 13;
	static final int UNDEFINED = -1;
	
	class SCCComponentInfo
	{
		int componentId;
		List<Integer> nodeids = null;
		
		public SCCComponentInfo() {
			nodeids= new ArrayList<Integer>();
		}
		
		void addNodeToComponent(int blockId) {
			nodeids.add(blockId);
		}
		
		public String ToStringComponentInfo(int BlockId) {
			String info = "";
			for(int index = 0; index < nodeids.size(); index++) {
				info += BlockId + " " + nodeids.get(index).toString() + "\n";
			}
			return info;
		}
	}
	
	static List<Integer> blocknos = null;
	static HashMap<Integer, BlockInfo> blockInfoMap = new HashMap<Integer, BlockInfo>();
	static int currentTime = 0;
	
	static PrintWriter writer;
	
	static int []low;
	static int []scc;
	static Boolean []discovered;
	static Boolean []processed;
	static int []parent;
	static int entryTime[];
	static int exitTime[];
	static Boolean isFinished;
	static BlockInfo currentBlock;
	static Stack<Integer> activeStack;
	static int componentsFound;
	
	SCCComponentInfo[] components = null;
	
	public void setComponentSize(int size) {
		components = new SCCComponentInfo[size];
		
		for(int index = 0; index < size; index++) {
			components[index] = new SCCComponentInfo();
		}
	}
	
	enum EdgeType {
		TREE,
		BACK,
		FORWARD,
		CROSS,
		INVALID
	};
	
	public HashMap<Integer, GraphInfo> nodes;
	BlockRange currentGraphRange;
	ArrayList<Integer> sortedNodeOrder;
	
	
	public BlockInfo(BlockRange _range) {
		nodes = new HashMap<Integer,GraphInfo>();
		currentGraphRange = new BlockRange(_range.lowerBound,
				_range.upperBound,_range.blockId);
		sortedNodeOrder = new ArrayList<Integer>();
	}
	
	public int findLogicalId(int longBlockId) {
		return longBlockId - currentGraphRange.lowerBound;
	}
	
	public int returnRealId(int logicalId) {
		return currentGraphRange.lowerBound + logicalId;
	}
	
	static void setCurrentBlock(BlockInfo block) {
		currentBlock = block;
	}
	
    static{
		try {
			writer= new PrintWriter("output.txt");
		} catch (FileNotFoundException e) {
			System.out.println("Problem reading blocks.txt");
			e.printStackTrace();
		}
    }
	
	static void initializeState(int numberOfNodes) {
		low = new int[numberOfNodes];
		scc = new int[numberOfNodes];
		discovered = new Boolean[numberOfNodes];
		parent = new int[numberOfNodes];
		processed = new Boolean[numberOfNodes];
		entryTime = new int[numberOfNodes];
		exitTime = new int[numberOfNodes];
		isFinished = false;
		activeStack = new Stack<Integer>();
		componentsFound = -1;
		
		for(int index = 0; index < numberOfNodes; index++) {
			low[index] = index;
			scc[index] = UNDEFINED;
			discovered[index] = false;
			processed[index] = false;
			parent[index] = UNDEFINED;
			entryTime[index] = UNDEFINED;
			exitTime[index] = UNDEFINED;
		}
	}
	
    static void readBlockFile(String fileName){
    	FileReader fileReader = null;
		try {
			fileReader = new FileReader(fileName);
		    BufferedReader bufferedReader =  new BufferedReader(fileReader);	       
		    blocknos= new ArrayList<Integer>();
		    while (true) {
		    	String line = null;
		    	line = bufferedReader.readLine();
					
		    	if (line == null) break;
		      
	            Integer value = Integer.parseInt(line.trim());
	            blocknos.add(value);
		    }
		   bufferedReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("Problem reading blocks.txt");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Problem reading blocks.txt");
			e.printStackTrace();
		}
    }
	
	public static BlockRange getBlockRange(int nodeId) {
		int low = 0;
		int high = blocknos.size();
		int middle = 0;    
		
		while(low <= high)
		{
			middle = (low + high)/2;
			
			if (middle == 0){
				return new BlockRange(0,blocknos.get(middle).intValue(),0); 
			}
			 
			if (nodeId <blocknos.get(middle)&& nodeId>=blocknos.get(middle -1)) {
				return new BlockRange(blocknos.get(middle - 1).intValue(),
						blocknos.get(middle).intValue(), middle);
			} 
			else if (nodeId >= blocknos.get(middle)){
				low = middle + 1;
		    }	
			else{
				high = middle -1; 
		   }
	   }
	   return new BlockRange(blocknos.get(middle).intValue(),UNDEFINED,middle - 1);
	}
	
	public static void parseBlockInfo(String fileName) throws Exception{
		FileReader fileReader = new FileReader(fileName);
		BufferedReader bufferedReader =  new BufferedReader(fileReader);
		
		int currentNode = -1;
		
		BlockRange currentRange =  null;
		GraphInfo  currGraphNode = null;
		
		while(true) {
			String line = bufferedReader.readLine();
			
            if (line == null) {
            	if(null != currGraphNode) {
            		if(!blockInfoMap.containsKey(currentRange.blockId)) {
            			blockInfoMap.put(currentRange.blockId, new BlockInfo(currentRange));
            		}
            		BlockInfo block = blockInfoMap.get(currentRange.blockId);
            		block.nodes.put(currentNode,currGraphNode);
            	}
            	break;
            }
            
            int source = Integer.parseInt(line.substring(SOURCEINDEXBEGIN,SOURCEINDEXEND).trim());
            int destination = Integer.parseInt(line.substring(DESTINDEXBEGIN,DESTINDEXEND).trim());
            
            if(currentNode != source){            	
            	if(null != currGraphNode) {
            		if(!blockInfoMap.containsKey(currentRange.blockId)) {
            			blockInfoMap.put(currentRange.blockId, new BlockInfo(currentRange));
            		}
            		BlockInfo block = blockInfoMap.get(currentRange.blockId);
            		block.nodes.put(currentNode,currGraphNode);
            	}
            	currentNode = source;
            	currentRange = getBlockRange(currentNode);
            	currGraphNode = new GraphInfo(source);
            }
            
            if((currentRange.upperBound != UNDEFINED
            		&& currentRange.lowerBound <= destination 
            		&& destination < currentRange.upperBound)
            		|| (currentRange.upperBound == UNDEFINED 
            			&& currentRange.lowerBound <= destination)) {
            	currGraphNode.addneighbour(destination);
            }
		}
		
        bufferedReader.close();
	}
	
	/*
	 * This Function classifies the edge based on the Graph Topology
	 * It classifies it into TREE, BACK, FORWARD, CROSS Edges
	 */
	static EdgeType edgeClassification(int x, int y) {
		if (parent[y] == x) 
			return EdgeType.TREE;
		
		if (discovered[y] && !processed[y]) 
			return EdgeType.BACK;
		
		if (processed[y] && (entryTime[y] > entryTime[x])) 
			return EdgeType.FORWARD;
		
		if (processed[y] && (entryTime[y] < entryTime[x])) 
			return EdgeType.CROSS;
		
		return EdgeType.INVALID;
	}
	
	static void pop_component(int source) {
		int logSourceId = currentBlock.findLogicalId(source);
		int t; 
		
		componentsFound = componentsFound + 1;
		scc[logSourceId] = componentsFound;
		
		while ((t = activeStack.pop()) != logSourceId) {
			scc[ t ] = componentsFound;
		}	
	}
	
	static void process_vertex_early(int logBlockId) {
		activeStack.push(logBlockId);
	}
	
	static void process_vertex_late(int source) {
		
		int logSourceId = currentBlock.findLogicalId(source);
		if (low[logSourceId] == logSourceId) { 
				pop_component(source);
		}
		
		if (parent[logSourceId] != -1 && entryTime[low[logSourceId]] < entryTime[low[parent[logSourceId]]])
			low[parent[logSourceId]] = low[logSourceId];
	}
	
	static void process_edge(int source, int destination) {
		
		int logSourceId = currentBlock.findLogicalId(source);
		int logDestId = currentBlock.findLogicalId(destination);
		
		EdgeType classify = edgeClassification(logSourceId,logDestId);
		if (classify == EdgeType.BACK) {
			if (entryTime[logDestId] < entryTime[ low[logSourceId] ] )
				low[logSourceId] = logDestId;
		}
		
		if (classify == EdgeType.CROSS) {
			if (scc[logDestId] == -1) /* component not yet assigned */
				if (entryTime[logDestId] < entryTime[ low[logSourceId] ] )
					low[logSourceId] = logDestId;
		}
	}
	
	static void Dfs(int blockId, GraphInfo node) {
		if(isFinished) {
			return;
		}
		
		if(null == node) {
			/*
			 * This means that the Graph has no outgoing edges
			 * It is a Sink. So just create a Temporary Node
			 * And process with assuming that there are no edges
			 */
			node = new GraphInfo(blockId);
		}
	
		int logBlockId = currentBlock.findLogicalId(blockId);
		discovered[logBlockId] = true;
		currentTime = currentTime + 1;
		entryTime[logBlockId] = currentTime;
	
		process_vertex_early(logBlockId);
	
		for(int index = 0; index < node.adjacentNodes.size(); index++) {
	
			int neighbourId = currentBlock.findLogicalId(node.adjacentNodes.get(index));
			int actualneighbourId = node.adjacentNodes.get(index);
	
			if(false == discovered[neighbourId]){
				parent[neighbourId] = logBlockId;
				process_edge(blockId, actualneighbourId);
				Dfs(actualneighbourId,currentBlock.nodes.get(actualneighbourId));
			}
			else {
				process_edge(blockId, actualneighbourId);
			}
		}
		process_vertex_late(blockId);
		currentTime = currentTime + 1;
		exitTime[logBlockId] = currentTime;
		processed[logBlockId] = true;
	}
	
	static void makeConnectedComponent(int BlockId){
		System.out.println("Making Connected Components for Block " + BlockId + " BEGIN");
		BlockInfo block = blockInfoMap.get(BlockId);
		currentBlock = block;
		
		if(null != block) {
			initializeState(block.currentGraphRange.upperBound - block.currentGraphRange.lowerBound);
			for(GraphInfo node: block.nodes.values()) {
				if(false == discovered[currentBlock.findLogicalId(node.nodeId)]) {
					Dfs(node.nodeId,node);
				}
			}
		}
		System.out.println("Block Id :" + BlockId + " Number of Connected Components: " + componentsFound);
		System.out.println("Making Connected Components for Block " + BlockId + " END");
		
		flushConnectedComponents();
	}
	
	static void flushConnectedComponents() {
		currentBlock.setComponentSize(componentsFound + 1 + 1);
		
		for(int index = 0; index < scc.length; index++) {
			if(scc[index] != -1) {
				SCCComponentInfo comp = currentBlock.components[scc[index]];
				comp.addNodeToComponent(currentBlock.returnRealId(index));
			}
			else {
				SCCComponentInfo comp = currentBlock.components[componentsFound + 1];
				comp.addNodeToComponent(currentBlock.returnRealId(index));
			}
		}
		
		currentBlock.constructedSortedNode();
		
		writer.println(currentBlock.ToStringComponentInfo());
	}
	
	ArrayList<Integer> rearrangeNodeList(ArrayList<Integer> uList) {
		ArrayList<Integer> tempList = new ArrayList<Integer>(uList);
		uList.clear();
		for(int index = 0; index < sortedNodeOrder.size(); index++) {
			if(tempList.contains(sortedNodeOrder.get(index))) {
				uList.add(sortedNodeOrder.get(index));
			}
		}
		return uList;
	}
	
	public static ArrayList<Integer> orderNodesInBlock(int BlockId, ArrayList<Integer> uList) {
		BlockInfo currentBlockInfo = blockInfoMap.get(BlockId);
		return currentBlockInfo.rearrangeNodeList(uList);
	}
	
	void constructedSortedNode() {
		for(int index = components.length - 1; index >= 0; index--) {
			SCCComponentInfo comp = components[index];
			
			for(int innerIndex = 0; innerIndex < comp.nodeids.size(); innerIndex++) {
				sortedNodeOrder.add(comp.nodeids.get(innerIndex));
			}
		}
	}
	
	String ToStringComponentInfo() {
		String componentInfo = "";		
		
		for(int index = components.length - 1; index >= 0; index--) {
			SCCComponentInfo comp = components[index];
			componentInfo += comp.ToStringComponentInfo(currentGraphRange.blockId);
		}
		return componentInfo;
	}
	
	public static void calculateConnectedComponents() {
		try {
			
			//Parse all the Edges from the File
			readBlockFile("input" + File.separator +"blocks.txt");
			parseBlockInfo("input" + File.separator +"edges.txt");
			
			//For Each Edge Make a Connected Component
			for(Entry<Integer, BlockInfo> entry : blockInfoMap.entrySet() ) {
				makeConnectedComponent(entry.getKey());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}