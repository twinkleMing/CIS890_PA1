import java.io.IOException;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;





class QueryRetrieval {
	MapFile.Reader Index;
        Hashtable<String, ArrayListWritableComparable> termstable
	
	public QueryRetrieval (Configuration config) throws IOException {
		FileSystem fs = FileSystem.get(config);
		Index = new MapFile.Reader(fs, fs.getWorkingDirectory() + "/part-00000", config);
                Hashtable<String, ArrayListWritableComparable> termstable = new Hashtable<String, ArrayListWritableComparable>();
	}
	
	public Hashtable<String, ArrayListWritableComparable> importQuery(String Query) throws IOException {

		Hashtable<String, ArrayListWritableComparable> termtable = new Hashtable<String, ArrayListWritableComparable>();
	    Query = Query.replaceAll("\\pP", " ");
	    StringTokenizer qterms = new StringTokenizer(Query);
	    
        while (qterms.hasMoreTokens()) {      	
        	String qterm= qterms.nextToken();
                char wordchars[] = qterm.toLowerCase().toCharArray();
	        Stemmer simword = new Stemmer();
	        simword.add(wordchars, wordchars.length);
	        simword.stem();
                qterm = simword.toString();
        	ArrayListWritableComparable<PairOfStringInt> docs = new ArrayListWritableComparable<PairOfStringInt>();
        	if (Index.get(new Text(qterm), docs) != null)
        		termtable.put(qterm, docs.clone());       	
        }
        return termtable;
	}
	
	public Hashtable<String, FloatWritable> computeMetric (Hashtable<String, ArrayListWritableComparable> termtable) {
		
		Hashtable<String, FloatWritable> docscores = new Hashtable<String, FloatWritable>();
		
		return docscores;
		
	}
}




