
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PA1_Test {

  public static class PreprocessMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

		
	    private Text docName = new Text();
	    private Text word = new Text();
	    private PairOfStrings worddocName = new PairOfStrings();
	    protected HashSet<String> stopWordsTable = null; 
	    protected Hashtable<String, Boolean> tagTable = null;
	    protected String split = null;
	    private Path stopWordsFile = null;
	    private static IntWritable one = new IntWritable(1);
  
	    public void setup(Context context)throws IOException {

	        FileSplit inSplit = (FileSplit)context.getInputSplit(); 
	        String fileName = inSplit.getPath().getName();
	    	if (stopWordsTable == null) {
	    		parseStopWords(context);  		
	    	}
	    	if (tagTable == null) {		
	    		tagTable = new Hashtable <String, Boolean>();
	    		tagTable.put(fileName, false);

	    	} 	
	    }
	    public void parseStopWords(Context context) throws IOException {

	    	stopWordsTable = new HashSet<String>();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			stopWordsFile = new Path(fs.getWorkingDirectory()+"/In/stopwords.txt");
	    	FSDataInputStream fis = fs.open(stopWordsFile);
	    	String words = null;
	        while ((words = fis.readLine()) != null) {
	        	stopWordsTable.add(words);
	        	}
	        fis.close();
	        }

	    public boolean checkTitleText(String line, String fileName, Context context) throws IOException, InterruptedException {
	  	
	    	String wordhere = "";
	    	StringTokenizer tokenizer = new StringTokenizer(line);
	    	if (tokenizer.hasMoreTokens()) {
	    		wordhere = tokenizer.nextToken().trim();   	    
	    	}
	    	
	        	if (tagTable.get(fileName) == true) {
	        		if (wordhere.equals("</TITLE>") || wordhere.equals("</TEXT>") ) {
	        			tagTable.put(fileName, false);
	        			return false;
	        		}
	        		else {
	        			return true;
	        		}
	        		
	        	}
	        	else {
	        		if (wordhere.equals("<TITLE>") || wordhere.equals("<TEXT>")) {
	        			tagTable.put(fileName, true);
	        		}
	        		
	        		
	        		return false;
	        	}
	        
	        }
	    
	    

		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			
	        String line =  value.toString(); 
	        FileSplit inSplit = (FileSplit)context.getInputSplit(); 
	        String fileName = inSplit.getPath().getName();
	        if (checkTitleText(line, fileName, context))
	        {
	        line = line.replaceAll("\\pP", "");
	        StringTokenizer tokenizer = new StringTokenizer(line);	    

	        docName.set(fileName);
	 
	        
	        while (tokenizer.hasMoreTokens()) {

	        	if (stopWordsTable == null)
	        		System.out.println("not prepared stop words table!");
	        	
	        	String token = tokenizer.nextToken();
	        		
	            if (!stopWordsTable.contains(token))
	            {
	        	char wordchars[] = token.toLowerCase().toCharArray();
	            Stemmer simword = new Stemmer();
	            simword.add(wordchars, wordchars.length);
	            simword.stem();
	            
	            word.set(simword.toString());
	            worddocName.set(word.toString(), docName.toString());
	            context.write(worddocName, one);
	            }
	        }
	        }
	    }
	        
		}
  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class PreprocessReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {

    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
  

	public static class InvertedIndexMapper extends Mapper<PairOfStrings, IntWritable, Text, PairOfStringInt> {

		Text word = new Text();
		Text docName = new Text();
		PairOfStringInt docNameNum = new PairOfStringInt();

		public void map(PairOfStrings key, IntWritable value,Context context) throws IOException, InterruptedException {
			
			word.set(key.getLeftElement());
			docName.set(key.getRightElement());
			docNameNum.set(docName.toString(), value.get());
			context.write(word, docNameNum);
		}

	}
	
	  public static class InvertedIndexReducer extends Reducer<Text, PairOfStringInt, Text, ArrayListWritableComparable> {
		 
		  
		    public void reduce(Text key, Iterable<PairOfStringInt> values, Context context) throws IOException, InterruptedException {
		    	 ArrayListWritableComparable<PairOfStringInt> docs = new ArrayListWritableComparable<PairOfStringInt>();
		      for (PairOfStringInt value : values) {
		        docs.add(value);
		      }
		      context.write(key, docs);
		    }
		  }
		  	
	  

	public static void runjob1(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf,"job1");
        //job.setJarByClass(PA1_Test.class);
	    job.setMapperClass(PreprocessMapper.class);
	    job.setCombinerClass(PreprocessReducer.class);
	    job.setReducerClass(PreprocessReducer.class);


	    job.setMapOutputKeyClass(PairOfStrings.class);
	    job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(IntWritable.class);


		// TODO: specify input and output DIRECTORIES (not files)
		 job.setInputFormatClass(TextInputFormat.class);
		 job.setOutputFormatClass(SequenceFileOutputFormat.class); 

		FileInputFormat.setInputPaths(job, new Path("In/In"));
		SequenceFileOutputFormat.setOutputPath(job, new Path("Out"));
//		job.setNumReduceTasks(10);		


		job.waitForCompletion(true);


	}
	
	public static void runjob2(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException{
		
		Job job = new Job(conf,"job2");
		job.setJarByClass(PA1_Test.class);
		// TODO: specify output types
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PairOfStringInt.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayListWritableComparable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		 job.setInputFormatClass(SequenceFileInputFormat.class);
		 job.setOutputFormatClass(SequenceFileOutputFormat.class); 

		SequenceFileInputFormat.setInputPaths(job, new Path("Out"));
		SequenceFileOutputFormat.setOutputPath(job, new Path("OutOut"));
		
   
	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setReducerClass(InvertedIndexReducer.class);
//	    job.setNumReduceTasks(10);
	    
	    job.waitForCompletion(true);	
	}

	public static void main(String[] args) throws Exception {
		
		Configuration PA = new Configuration();
		runjob1(PA);	
		runjob2(PA);
	}


}


