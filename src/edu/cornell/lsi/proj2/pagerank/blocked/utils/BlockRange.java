package edu.cornell.lsi.proj2.pagerank.blocked.utils;

public class BlockRange
{
	public int upperBound;
	public int lowerBound;
	int blockId;
	
	public BlockRange(int lowerBound, int upperBound, int blockId)
	{
		this.upperBound = upperBound;
		this.lowerBound = lowerBound;
		this.blockId = blockId;
	}
}