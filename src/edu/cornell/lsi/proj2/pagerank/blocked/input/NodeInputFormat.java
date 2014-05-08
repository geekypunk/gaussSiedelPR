package edu.cornell.lsi.proj2.pagerank.blocked.input;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;

import edu.cornell.lsi.proj2.pagerank.blocked.domain.Node;


public class NodeInputFormat extends FileInputFormat<IntWritable, Node> {
    public RecordReader<IntWritable, Node> createRecordReader(InputSplit input, TaskAttemptContext ctx) throws IOException {
	return new NodeRecordReader(); //Simply construct and return a NodeRecordReader
    }
}