package edu.cornell.lsi.proj2.pagerank.blocked.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;


public class Utility {

    static double fromNetID;
    static double rejectMin;
    static double rejectLimit;
    static List<Integer> blocknos = null;
    
    public Utility(){}
    
    static{
    	FileReader fileReader = null;
		try {
			fileReader = new FileReader("input/blocks.txt");
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
    
	public static void main(String[] args) throws IOException {
		System.out.println(blockIDofNode(10327)); 
	}
	
	public static long blockIDofNode(long nodeID) throws IOException
	{	   
	   //Binary search to find the block number
	   int p= 0 , q=blocknos.size(),m=0; 

	   while(p<=q)
	   {
		   m= (p+q)/2;
		   if (m==0)
		   {
			   return m; 
		   }

		   if (nodeID<blocknos.get(m)&& nodeID>=blocknos.get(m-1)) 
		   {
				return m;
		   } 
		   else if (nodeID>=blocknos.get(m))
		   {
				p= m+1;
		   }	
		   else
		   {
				q=m-1; 
		   }
	   }
	   return m;
	}

	public static void filterEdges(String inputPath, String sourceFilePath, String destFilePath) throws IOException
	{
		FileReader fileReader = new FileReader(inputPath+sourceFilePath);
		BufferedReader bufferedReader =  new BufferedReader(fileReader);

		int count =0 ;
		int origCount=0 ;
		PrintWriter writer = new PrintWriter(inputPath+destFilePath, "UTF-8");
		
		while (true) {
			origCount++;

			String line = bufferedReader.readLine();
			if (line == null) break;

			String weight= line.substring(13).trim();

			double value = Double.parseDouble(weight);
			boolean ans=selectInputLine(value);

			if (ans== true)
			{	
				count++;
				writer.println(line);
			}
		}
		System.out.println("original Edge Count: "+origCount+"selected edge count: "+count);

		bufferedReader.close();
		writer.close();
	}
	
	public static void generateEdgeInputFile(String inputPath, String filterFilePath, String dbFormatFilePath) throws IOException
	{
		FileReader fileReader = new FileReader(inputPath+filterFilePath);
	    BufferedReader bufferedReader =  new BufferedReader(fileReader);   

	    PrintWriter writer = new PrintWriter(inputPath+dbFormatFilePath, "UTF-8");
	    
	    ArrayList<Integer> currEdgeNodeList= new ArrayList<Integer>();
	    
	    double pagerank= 1/(double)685230;
	    
        String line1 = bufferedReader.readLine();
        if (line1 == null) 
        {
    	    bufferedReader.close();
    	    writer.close();
        	return;
        }
        String from1= line1.substring(0, 7).trim();
        String to1= line1.substring(7,12).trim();
	    
        Integer curr_node = Integer.parseInt(from1);
        currEdgeNodeList.add(Integer.parseInt(to1));
        
        Integer next_node; 
        
        int expectedNextNumber = curr_node + 1;
         
	    while(true)
	    {	
	    	String line = bufferedReader.readLine();
	        if (line == null) 
	        	break;
	        String from= line.substring(0, 6).trim();
	        String to= line.substring(6,13).trim();
	        next_node= Integer.parseInt(from);
	        
	    	if (next_node.equals(curr_node))
	    	{
	    		currEdgeNodeList.add(Integer.parseInt(to));
	    	}
	    	else
	    	{
	    		String nodeLine= curr_node+" "+pagerank+" "; 
	    		for(Integer i : currEdgeNodeList)
	    		{
	    			nodeLine+=i+",";
	    		}
	    		nodeLine = nodeLine.substring(0, nodeLine.length()-1);
	    		writer.println(nodeLine);  
	    		
	    		while(expectedNextNumber != next_node)
	    		{
	    			String temp= expectedNextNumber+" "+pagerank+" "; 
	    			expectedNextNumber++;
		    		writer.println(temp);  
	    		}
	    		
	    		expectedNextNumber++;
		    		
	    		curr_node= next_node; 
	    		currEdgeNodeList= new ArrayList<Integer>();
	    		currEdgeNodeList.add(Integer.parseInt(to));	
	    		
	    	}
	    }
	    
	    if(currEdgeNodeList.size() != 0) {
	    	String nodeLine= curr_node+" "+pagerank+" "; 
    		for(Integer i : currEdgeNodeList)
    		{
    			nodeLine+=i+",";
    		}
    		nodeLine = nodeLine.substring(0, nodeLine.length()-1);
    		writer.println(nodeLine);  
	    }
	    
	    while(expectedNextNumber <= 685228) {
	    	String temp= expectedNextNumber+" "+pagerank+" "; 
			expectedNextNumber++;
    		writer.println(temp);  
	    }
	    
	    bufferedReader.close();
	    writer.close();
	    System.out.println("closed");
	}
	
	public static boolean selectInputLine(double x) {
		return ( ((x >= rejectMin) && (x < rejectLimit)) ? false : true );
	}

	public static void initialize(double netId) {
		 fromNetID=netId;
		 rejectMin= 0.99*fromNetID;
		 rejectLimit= rejectMin+0.01;
		
	}

}
