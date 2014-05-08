package edu.cornell.lsi.proj2.pagerank.blocked.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.cornell.lsi.proj2.pagerank.blocked.domain.Edge;
import edu.cornell.lsi.proj2.pagerank.blocked.domain.Node;
import edu.cornell.lsi.proj2.pagerank.blocked.domain.NodeOrEdge;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.ApplicationConstants;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.Utility;

public class BlockPageRankMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrEdge>{
	
	
	protected void map(IntWritable key, Node value, Context context)
			throws IOException, InterruptedException {
		
		int nodeId = value.getNodeid();
		double pageRank = value.getPageRank();
		int[] outNodes = value.getOutgoing();
		int degree = outNodes.length;

		int blockID = (int) Utility.blockIDofNode(nodeId);
		
		context.write(new IntWritable(blockID), new NodeOrEdge(value));
		
	
		for (int out : outNodes) {
			int blockIDOut = (int) Utility.blockIDofNode(out);;
			Edge e = null;
			if (blockIDOut == blockID) {
				e = new Edge("BE",nodeId, out, -1);
			
			} else {
				double pageRankFactor = (pageRank / (double)degree);
				e = new Edge("BC",nodeId, out, pageRankFactor);
			}
			context.write(new IntWritable(blockIDOut), new NodeOrEdge(e));
		}
	
	}
}
