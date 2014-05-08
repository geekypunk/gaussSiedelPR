package edu.cornell.lsi.proj2.pagerank.blocked.domain;
import org.apache.hadoop.io.Writable;
import java.io.*;

public class NodeOrEdge implements Writable{
    private Node n;
    private Edge e;
    private boolean is_node;

    //Used for internal Hadoop purposes only. 
    //Do not use this constructor!
    public NodeOrEdge() {
	is_node = false;
	e = null;
    }

    //Construct a NodeOrEdge that is a node.
    public NodeOrEdge(Node n) {
	this.n = n;
	is_node = true;
    }
    
    //Construct a NodeOrEdge that is a Edge
    public NodeOrEdge(Edge e) {
	this.e = e;
	is_node = false;
    }

    //Find out whether this is actually a Node or not
    //If not, it's an Edge
    public boolean isNode() {
	return is_node;
    }

    //If this is a Node, return it.
    //Otherwise, return null
    public Node getNode() {
	if(!isNode()) return null;
	return n;
    }
    
    //If this is a Edge, return it.
    //Otherwise, return null
    public Edge getEdge() {
	if(isNode()) return null;
	return e;
    }

    //Used for internal Hadoop purposes only
    //Describes how to write NodeOrEdge objects across a network
    public void write(DataOutput out) throws IOException {
	out.writeBoolean(is_node);
	if(is_node) {
	    n.write(out);
	}
	else {
	    e.write(out);
	}
    }

    //Used for internal Hadoop purposes only
    //Describes how to read NodeOrEdge objects from across a network
    public void readFields(DataInput in) throws IOException {
	is_node = in.readBoolean();
	if(is_node) {
	    n = new Node(-1); //just to avoid errors --- wish this was static
	    n.readFields(in);
	} else {
		e = new Edge();	//CHECK THIS
	    e.readFields(in);
	}
    }
}
