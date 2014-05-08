package edu.cornell.lsi.proj2.pagerank.blocked.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Edge implements Writable{
	String edgeType;
	private int sourceNodeId;
	private int destNodeId;
	private double pageRankMass;
	
	
	public Edge(){}
	
	public Edge(String edgeType, int sourceNodeId, int destNodeId,
			double pageRankMass) {
		super();
		this.edgeType = edgeType;
		this.sourceNodeId = sourceNodeId;
		this.destNodeId = destNodeId;
		this.pageRankMass = pageRankMass;
	}


	public int getSourceNodeId() {
		return sourceNodeId;
	}

	public void setSourceNodeId(int sourceNodeId) {
		this.sourceNodeId = sourceNodeId;
	}

	public int getDestNodeId() {
		return destNodeId;
	}

	public void setDestNodeId(int destNodeId) {
		this.destNodeId = destNodeId;
	}

	public double getPageRankMass() {
		return pageRankMass;
	}

	public void setPageRankMass(double pageRankMass) {
		this.pageRankMass = pageRankMass;
	}

	public String getEdgeType() {
		return edgeType;
	}

	public void setEdgeType(String edgeType) {
		this.edgeType = edgeType;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		edgeType = WritableUtils.readString(in);
		sourceNodeId = in.readInt();
		destNodeId = in.readInt();
		pageRankMass = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//out.writeBytes(edgeType);
		WritableUtils.writeString(out, edgeType);
		out.writeInt(sourceNodeId);
		out.writeInt(destNodeId);
		out.writeDouble(pageRankMass);
		//out.writeInt(-1);
		
	}
}
