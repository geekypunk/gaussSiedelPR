package edu.cornell.lsi.proj2.pagerank.blocked;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.cornell.lsi.proj2.pagerank.blocked.domain.NodeOrEdge;
import edu.cornell.lsi.proj2.pagerank.blocked.input.NodeInputFormat;
import edu.cornell.lsi.proj2.pagerank.blocked.map.BlockPageRankMapper;
import edu.cornell.lsi.proj2.pagerank.blocked.output.NodeOutputFormat;
import edu.cornell.lsi.proj2.pagerank.blocked.reduce.BlockPageRankReducer;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.ApplicationConstants;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.BlockInfo;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.Counters;
import edu.cornell.lsi.proj2.pagerank.blocked.utils.Utility;


public class BlockedPageRank {

	
	public static void main(String[] args) throws IOException {

		//String inputPath = "input/processedEdges647.txt";
		
		if(args.length!=3){
			throw new IllegalArgumentException("Usage: netid inputBucket outputBucket");
		}
		
		double  netId = Double.parseDouble(args[0]);
		Utility.initialize(netId);
		
		/*
		inputBucket = args[1];
		outputBucket = args[2];
		*/
		
		/*String inputPath = "input/inputPR.txt";
		String outputPath = "stage/iterations";*/
		
		String inputBasePath = args[1]+File.separator;
		String outputBasePath = args[2]+File.separator;
		
		//local code
		String outputPath = outputBasePath+"iterations";
		//local code end
		String filterFilePath = "filterEdges.txt";
		String dbFormatFilePath = "dbFormatInput.txt";
		
		/*Utility.filterEdges(inputBasePath,"edges.txt",filterFilePath);
		System.out.println("Created filtered edges file");
		
		Utility.generateEdgeInputFile(inputBasePath,filterFilePath,dbFormatFilePath);
		System.out.println("Created db format file");*/
		
		String inputPath = inputBasePath+dbFormatFilePath;
		cleanDirectory();
		
		BlockInfo.calculateConnectedComponents();

		int i = 0;
		double averageResidualError = 0;
		double residualError = 0;
		do{
			try{
			
				Job job = new Job();
	            
				// Set a unique job name
	            job.setJobName("blockedPageRankIteration_"+ i);
	            job.setJarByClass(BlockedPageRank.class);
	            
	            // Set Mapper and Reducer class
	           
	            job.setMapperClass(BlockPageRankMapper.class);
	            job.setReducerClass(BlockPageRankReducer.class);
	
	            // set the classes for output key and value
	            job.setOutputKeyClass(IntWritable.class);
	            job.setOutputValueClass(NodeOrEdge.class);
	            
	            job.setInputFormatClass(NodeInputFormat.class); //We take in <Int,Node> pairs
	        	job.setOutputFormatClass(NodeOutputFormat.class);
	        	
	            // on the initial pass, use the preprocessed input file
	            // note that we use the default input format which is TextInputFormat (each record is a line of input)
	            if (i == 0) {
	                FileInputFormat.addInputPath(job, new Path(inputPath)); 	
	            // otherwise use the output of the last pass as our input
	            } else {
	            	FileInputFormat.addInputPath(job, new Path(outputPath + File.separator+"iteration"+i)); 
	            }
	            // set the output file path
	            String currOutputPath = outputPath + File.separator+"iteration"+(i+1);
	            FileOutputFormat.setOutputPath(job, new Path(currOutputPath));
	            
	            job.getConfiguration().set("OutputPath", currOutputPath);
	            
	            // execute the job and wait for completion before starting the next pass
	            job.waitForCompletion(true);
	            
	            // before starting the next pass, compute the avg residual error for this pass and print it out
	            residualError = job.getCounters().findCounter(Counters.RESIDUAL_ERROR).getValue() / (double)ApplicationConstants.HADOOP_COUNTER_OFFSET;
	            //residualError = Double.longBitsToDouble(job.getCounters().findCounter(Counters.RESIDUAL_ERROR).getValue());
	             
	            averageResidualError =   residualError /(double) ApplicationConstants.TOTAL_NUMBER_OF_BLOCKS;

	            long averagenumberOfIterations = job.getCounters().findCounter(Counters.NUMBER_OF_ITERATIONS).getValue();
	            double averagenumberOfIterationsDouble = (double) averagenumberOfIterations/ ApplicationConstants.TOTAL_NUMBER_OF_BLOCKS;
	            System.out.println("Average residual error for iteration "+i+": is "+averageResidualError);
	            System.out.println("Average number of iterations per block for iteration "+i+": is "+averagenumberOfIterationsDouble);
	            
	            // reset the counter for the next round
	            job.getCounters().findCounter(Counters.RESIDUAL_ERROR).setValue(0L);
	            job.getCounters().findCounter(Counters.NUMBER_OF_ITERATIONS).setValue(0L);
	            i++;
			}catch(Exception e){
				
				e.printStackTrace();
			}
			
		}while(averageResidualError > ApplicationConstants.TERMINATION_RESIDUAL);
	}

	private static void cleanDirectory() {
		File directory = new File("stage");
		try{
			 
            delete(directory);

        }catch(IOException e){
            e.printStackTrace();
            System.exit(0);
        }
		
	}
	public static void delete(File file)
	    	throws IOException{
	 
	    	if(file.isDirectory()){
	 
	    		//directory is empty, then delete it
	    		if(file.list().length==0){
	 
	    		   file.delete();
	    		   System.out.println("Directory is deleted : " 
	                                                 + file.getAbsolutePath());
	 
	    		}else{
	 
	    		   //list all the directory contents
	        	   String files[] = file.list();
	 
	        	   for (String temp : files) {
	        	      //construct the file structure
	        	      File fileDelete = new File(file, temp);
	 
	        	      //recursive delete
	        	     delete(fileDelete);
	        	   }
	 
	        	   //check the directory again, if empty then delete it
	        	   if(file.list().length==0){
	           	     file.delete();
	        	     System.out.println("Directory is deleted : " 
	                                                  + file.getAbsolutePath());
	        	   }
	    		}
	 
	    	}else{
	    		//if file, then delete it
	    		file.delete();
	    		System.out.println("File is deleted : " + file.getAbsolutePath());
	    	}
	    }
	
}
