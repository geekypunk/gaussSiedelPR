package edu.cornell.lsi.proj2.pagerank.blocked.utils;
import java.util.ArrayList;
import java.util.List;

public class GraphInfo
{
	public int nodeId;
	public List<Integer> adjacentNodes;
		
	public GraphInfo(int nodeId) {
		this.nodeId = nodeId;
		adjacentNodes = new ArrayList<Integer>();
	}
		
	public void addneighbour(int nodeId){
		this.adjacentNodes.add(nodeId);
	}
}