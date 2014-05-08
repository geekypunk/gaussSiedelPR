package edu.cornell.lsi.proj2.pagerank.blocked.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.cornell.lsi.proj2.pagerank.blocked.domain.Edge;
import edu.cornell.lsi.proj2.pagerank.blocked.domain.Node;
import edu.cornell.lsi.proj2.pagerank.blocked.domain.NodeOrEdge;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.ApplicationConstants;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.BlockInfo;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.Counters;


public class BlockPageRankReducer extends Reducer<IntWritable, NodeOrEdge, IntWritable, Node> {
	
	private Map<Integer, Double> prevMRPRMap = new HashMap<Integer, Double>();
	private Map<Integer, Double> prevItrPRMap = new HashMap<Integer, Double>();
	private Map<Integer, Double> currItrPRMap = new HashMap<Integer, Double>();
	private Map<Integer, ArrayList<Integer>> BE = new HashMap<Integer, ArrayList<Integer>>();
	private Map<Integer, Double> BC = new HashMap<Integer, Double>();
	private Map<Integer, Node> nodeIdToNodeMap = new HashMap<Integer, Node>();
	private List<Integer> nodeList = new ArrayList<Integer>();
	
	private double dampingFactor =  0.85;
	private double randomJumpContribution = (1 - dampingFactor) /(double) ApplicationConstants.TOTAL_NUMBER_OF_NODES;
	private int maximumIterations = 500;
	long numberOfIterations = 0;
	
	
	protected void reduce(IntWritable key, Iterable<NodeOrEdge> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<NodeOrEdge> itr = values.iterator();
		NodeOrEdge input = null;
		
		double residualError =  0.0;
		Integer nodeWithMaxId = 0;
		
		ArrayList<Integer> incomingEdgesList = new ArrayList<Integer>();
		double pageRankAccumulatorForBC = 0.0;
		clearDataStructures();	
		
		while (itr.hasNext()) {
			input = itr.next();
			if (input.isNode()) {
				Node node = input.getNode();
				int currNodeId = node.getNodeid();
				prevItrPRMap.put(node.getNodeid(), node.getPageRank());
				int[] neighborList = node.getOutgoing();
				String neighborListStr = Arrays.toString(neighborList);
				neighborListStr = neighborListStr.substring(1,neighborListStr.length()-1);
				neighborListStr = neighborListStr.replaceAll("\\s+", "");
				
				node.setNeighborList(neighborListStr);
				node.setOutDegree(neighborList.length);
				
				nodeList.add(currNodeId);
				nodeIdToNodeMap.put(currNodeId, node);
				if (currNodeId > nodeWithMaxId) {
					nodeWithMaxId = currNodeId;
				}
			} else  {
				Edge e = input.getEdge();
				
				if("BE".equals(e.getEdgeType())){
					if (BE.containsKey(e.getDestNodeId())) { 
						incomingEdgesList = BE.get(e.getDestNodeId());
					} else {
						incomingEdgesList = new ArrayList<Integer>();
					}
					incomingEdgesList.add(e.getSourceNodeId()); 
					BE.put(e.getDestNodeId(), incomingEdgesList); 
				}else if("BC".equals(e.getEdgeType())){
					if (BC.containsKey(e.getDestNodeId())) {
						pageRankAccumulatorForBC = BC.get(e.getDestNodeId());
					} else {
						pageRankAccumulatorForBC = 0.0;
					}
					pageRankAccumulatorForBC += e.getPageRankMass();
					BC.put(e.getDestNodeId(), pageRankAccumulatorForBC);
				}
			}	
		}
		copyMap(prevMRPRMap,prevItrPRMap);
		copyMap(currItrPRMap,prevItrPRMap);
		int i = 0;
		do {
		
			i++;
			residualError = iterateBlockOnce(key.get());
			copyMap(prevItrPRMap, currItrPRMap);
			//currItrPRMap.clear();

		} while (i < maximumIterations && residualError > ApplicationConstants.TERMINATION_RESIDUAL);
		//} while (residualError > ApplicationConstants.TERMINATION_RESIDUAL);
		
		residualError = 0.0;
		for (Integer vertex : nodeList) {
			Node node = nodeIdToNodeMap.get(vertex);
			residualError += Math.abs(node.getPageRank() - prevItrPRMap.get(vertex)) /(double) prevItrPRMap.get(vertex);
		}
		residualError = residualError /(double) nodeList.size();
		long residualAsLong = (long) Math.floor(residualError * ApplicationConstants.HADOOP_COUNTER_OFFSET);
		context.getCounter(Counters.RESIDUAL_ERROR).increment(residualAsLong);
		
		context.getCounter(Counters.NUMBER_OF_ITERATIONS).increment(numberOfIterations);
		for (int v : nodeList) {
			Node node = nodeIdToNodeMap.get(v);
			node.setPageRank(prevItrPRMap.get(node.getNodeid()));
			
			context.write(new IntWritable(v), node);
			if (v==nodeWithMaxId) {
				System.out.println("Block:" + key + " node:" + v + " pageRank:" + prevItrPRMap.get(v));
			}
		}
			
		cleanup(context);
	}

	private void copyMap(Map<Integer, Double> prevItrPRMap,
			Map<Integer, Double> currItrPRMap) {
		
		for(Integer currId: currItrPRMap.keySet()){
			prevItrPRMap.put(currId, currItrPRMap.get(currId));
		}
	}

	private void clearDataStructures() {
		nodeList.clear();
		prevItrPRMap.clear();
		BE.clear();
		BC.clear();
		nodeIdToNodeMap.clear();
		prevMRPRMap.clear();
		currItrPRMap.clear();
		numberOfIterations=0;
	}

	protected double iterateBlockOnce(int blockId) {
		numberOfIterations++;
		ArrayList<Integer> uList = new ArrayList<Integer>();
		double npr = 0.0;
		double r = 0.0;
		double residualError = 0.0;
		nodeList = BlockInfo.orderNodesInBlock(blockId, (ArrayList<Integer>) nodeList);
		for (Integer nodeId : nodeList) {
			npr = 0.0;
			double prevPR = prevItrPRMap.get(nodeId);
			//double prevPR = currItrPRMap.get(nodeId);
			if (BE.containsKey(nodeId)) {
				uList = BE.get(nodeId);
				for (Integer u : uList) {
					Node currNode = nodeIdToNodeMap.get(u);
					npr += (currItrPRMap.get(u) /(double) currNode.getOutDegree());
				}
			}
			if (BC.containsKey(nodeId)) {
				r = BC.get(nodeId);
				npr += r;
			}

			npr = (dampingFactor * npr) + randomJumpContribution;
			currItrPRMap.put(nodeId, npr);
			residualError += Math.abs(prevPR - npr) /(double) npr;
		}
		residualError = residualError /(double) nodeList.size();
		return residualError;
	}

}

