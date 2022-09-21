import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Max_frec {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text void_key = new Text("Manco_Lepanto");
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    context.write(void_key, value);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
	  
    private String input = new String();
	//private Text win_word = new Text("STD");
	private Text win_count=new Text("0");


    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	Text win_word = new Text("STD");
    	for(Text val:values){
    		String word;
    		int count;
	    	input=val.toString();
	    	
	    	System.out.print(input+" /Lenght: ");
	    	String [] keyValue=input.split("\\s+");
	    	System.out.println(keyValue.length);
	    	
	    	System.out.print("Word: ");
	    	word=keyValue[0];
	    	System.out.println(word.toString());
	    	
	    	System.out.print("Value: ");
	    	count=Integer.valueOf(keyValue[keyValue.length-1]);
	    	System.out.println(count);
	    	
	    	if (count>Integer.valueOf(win_count.toString())){
		    	System.out.print("**Mayor**");
	    		win_count.set(keyValue[keyValue.length-1]);
	    		win_word.set(keyValue[0]);
	    		System.out.println(win_word.toString());
	    	}
	    }
    	context.write(win_word, win_count);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Max_frec");
    job.setJarByClass(Max_frec.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setNumReduceTasks(0);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
