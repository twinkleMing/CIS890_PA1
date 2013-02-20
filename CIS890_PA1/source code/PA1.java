
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PA1 {
	
  private static final int N=1400;
  public static class PreprocessMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, PairOfStrings, IntWritable> {

		
	    private Text docName = new Text();
	    private Text word = new Text();
	    private PairOfStrings docNameword = new PairOfStrings();
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
	        line = line.replaceAll("\\pP", " ");
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
	            docNameword.set(docName.toString(),word.toString());
	            context.write(docNameword, one);
	            }
	        }
	        }
	    }
	        
		}
  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class PreprocessReducer extends org.apache.hadoop.mapreduce.Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {

    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
  

	public static class ComputeTFMapper extends org.apache.hadoop.mapreduce.Mapper<PairOfStrings, IntWritable, Text, PairOfStringInt> {

		Text word = new Text();
		Text docName = new Text();
		PairOfStringInt wordNum = new PairOfStringInt();

		public void map(PairOfStrings key, IntWritable value, Context context) throws IOException, InterruptedException {
			
			docName.set(key.getLeftElement());
			word.set(key.getRightElement());
			wordNum.set(word.toString(), value.get());
			context.write(docName, wordNum);
		}
	}
	
	  public static class ComputeTFReducer extends org.apache.hadoop.mapreduce.Reducer<Text, PairOfStringInt, Text, PairOfStringDouble> {
		 
		  
		    public void reduce(Text key, Iterable<PairOfStringInt> values, Context context) 
		    throws IOException, InterruptedException {
		    	 ArrayListWritableComparable<PairOfStringInt> backup = new ArrayListWritableComparable<PairOfStringInt>();
		    	 int max = 0;
		         for (PairOfStringInt value : values)  { 
		        	 int current = value.getRightElement();
		        	 if (max<current) max = current;
		        	 backup.add(value.clone());
		         }
		         //context.write(key, new IntWritable(max));
		         
		         for(PairOfStringInt value:backup) {
		    		 PairOfStringDouble tf = new PairOfStringDouble();
		    		 double tfscore = ((double)value.getRightElement())/((double)max); 
		    		 tf.set(value.getLeftElement(), tfscore);
		    		 context.write(key, tf);     		     
		      }	 		          
		    }
		  }
		  	

		public static class docLengthMapper extends MapReduceBase implements Mapper<Text, PairOfStringDouble, Text, PairOfStringDouble> {
			Text docName = new Text();
			PairOfStringDouble wordTFIDF = new PairOfStringDouble();
			public void map(Text key, PairOfStringDouble value, OutputCollector<Text, PairOfStringDouble> collector, Reporter reporter) throws IOException {
				docName.set(value.getLeftElement());
				wordTFIDF.set(key.toString(), value.getRightElement());
				collector.collect(docName, wordTFIDF);
			}

		}

	  
	    public static class docLengthReducer extends MapReduceBase implements Reducer<Text, PairOfStringDouble, Text, DoubleWritable> {
					 
				  
		    public void reduce(Text key, Iterator<PairOfStringDouble> values, OutputCollector<Text, DoubleWritable> collector, Reporter reporter) throws IOException {
		    	double dlength = (double) 0.0;		    	
		    	while (values.hasNext()) {
		    		double temp = values.next().getRightElement();
		    		dlength += temp*temp;
		    	}		    			    		
		    	collector.collect(key, new DoubleWritable(java.lang.Math.sqrt(dlength)));
		    }
	    }
	    
		public static class InvertedIndexMapper extends MapReduceBase implements Mapper<Text, PairOfStringDouble, Text, PairOfStringDouble> {
			public void map(Text key, PairOfStringDouble value, OutputCollector<Text, PairOfStringDouble> collector, Reporter reporter) throws IOException {
				collector.collect(key, value);
			}
		}

	  
	    public static class InvertedIndexReducer extends MapReduceBase implements Reducer<Text, PairOfStringDouble, Text, ArrayListWritableComparable> {					 				  
		    public void reduce(Text key, Iterator<PairOfStringDouble> values, OutputCollector<Text, ArrayListWritableComparable> collector, Reporter reporter) throws IOException {
		    	ArrayListWritableComparable<PairOfStringDouble>	docs = new ArrayListWritableComparable<PairOfStringDouble>();
		    	while (values.hasNext()) {
		    		docs.add(values.next().clone());
		    	}		    		
		    	collector.collect(key, docs);
		    }
	    }
	    
		public static class computeTFIDFMapper extends org.apache.hadoop.mapreduce.Mapper<Text, PairOfStringDouble, Text, PairOfStringDouble> {
            Text word = new Text();
            PairOfStringDouble docNameTF = new PairOfStringDouble();
			public void map(Text key, PairOfStringDouble value, Context context) throws IOException, InterruptedException  {
				word.set(value.getLeftElement());
				docNameTF.set(key.toString(), value.getRightElement());
				context.write(word,docNameTF);
			}
		}

	    public static class computeTFIDFReducer extends org.apache.hadoop.mapreduce.Reducer<Text, PairOfStringDouble, Text, PairOfStringDouble> {
			 
			PairOfStringDouble docTFIDF = new PairOfStringDouble();  
		    public void reduce(Text key, Iterable<PairOfStringDouble> values, Context context) throws IOException, InterruptedException  {
		    	ArrayListWritableComparable<PairOfStringDouble> backup = new ArrayListWritableComparable<PairOfStringDouble>();
		    	int docNum = 0;
		    	for (PairOfStringDouble value: values) {
		    		docNum++;
		    		backup.add(value.clone());
		    	}
	    		double IDF = (java.lang.Math.log(N/docNum))/(java.lang.Math.log(2));
		    	for (PairOfStringDouble value: backup)  {
		    		double TFIDF = value.getRightElement()*IDF; 
		    		docTFIDF.set(value.getLeftElement(), TFIDF);	
		    		context.write(key, docTFIDF);		    		
		    	}
		    	//context.write(key, new DoubleWritable(IDF));
		    	
		    	
		    }
	    }

	
	public static void runjob1(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf,"preprocess");
        job.setJarByClass(PA1.class);
	    job.setMapperClass(PreprocessMapper.class);
	    job.setCombinerClass(PreprocessReducer.class);
	    job.setReducerClass(PreprocessReducer.class);

	    job.setMapOutputKeyClass(PairOfStrings.class);
	    job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);		 
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class); 
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, new Path("In/cranfieldDocs"));
		org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.setOutputPath(job, new Path("OutPreprocess"));
		 
		job.waitForCompletion(true);

	}

	public static void runjob2(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf,"computescore");
        job.setJarByClass(PA1.class);
	    job.setMapperClass(ComputeTFMapper.class);
	   // job.setCombinerClass(ComputeTFReducer.class);
	    job.setReducerClass(ComputeTFReducer.class);


	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PairOfStringInt.class);
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PairOfStringDouble.class);


		// TODO: specify input and output DIRECTORIES (not files)
		 job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);		 
		 job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class); 

		 org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.setInputPaths(job, new Path("OutPreprocess"));
		 org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.setOutputPath(job, new Path("OutComputeTF"));

		job.waitForCompletion(true);


	
	}	
	public static void runjob3(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = new Job(conf,"computTFIDF");
        job.setJarByClass(PA1.class);
	    job.setMapperClass(computeTFIDFMapper.class);
	    //job.setCombinerClass(computeTFIDFReducer.class);
	    job.setReducerClass(computeTFIDFReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PairOfStringDouble.class);
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PairOfStringDouble.class);

		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);		 
		job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class); 
		org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.setInputPaths(job, new Path("OutComputeTF"));
		org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.setOutputPath(job, new Path("OutComputeTFIDF"));
		 
		job.waitForCompletion(true);

	}
	
	public static void runjob4(Configuration conf) throws IOException{
	    JobConf job = new JobConf(conf,PA1.class);
	    job.setJobName("getdoclength");
	        
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PairOfStringDouble.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);	     
	    
	    
	    job.setMapperClass(docLengthMapper.class);
	    job.setReducerClass(docLengthReducer.class);
	        
	    job.setInputFormat(SequenceFileInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	        
	    SequenceFileInputFormat.setInputPaths(job, new Path("OutComputeTFIDF"));
	    FileOutputFormat.setOutputPath(job, new Path("OutDocLength"));
	        
	    JobClient.runJob(job);

	}
	
	public static void runjob5(Configuration conf) throws IOException{
	    JobConf job = new JobConf(conf,PA1.class);
	    job.setJobName("invertedindex");
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(PairOfStringDouble.class);	        
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(ArrayListWritableComparable.class);
	        
	    job.setMapperClass(InvertedIndexMapper.class);
	    job.setReducerClass(InvertedIndexReducer.class);
	        
	    job.setInputFormat(SequenceFileInputFormat.class);
	    job.setOutputFormat(TextOutputFormat.class);
	        
	    SequenceFileInputFormat.setInputPaths(job, new Path("OutComputeTFIDF"));
	    FileOutputFormat.setOutputPath(job, new Path("OutInvertedIndex"));
	        
	    JobClient.runJob(job);

	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration PA = new Configuration();
		//runjob1(PA);	
		//runjob2(PA);
		runjob3(PA);
		//runjob4(PA);
		//runjob5(PA);
	}


}

