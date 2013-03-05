package org.apache.hama.mapreduce.examples;

import java.io.IOException;
import java.net.URL;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.mapreduce.MapRedBSPJob;
import org.apache.hama.mapreduce.Mapper;
import org.apache.hama.mapreduce.Reducer;

import edu.stanford.nlp.parser.lexparser.LexicalizedParser;

public class WordCount {

  public static class WordCountMapper extends
  Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private final static Text word = new Text();
    private static LexicalizedParser lp;
    static{
      URL url = WordCountMapper.class.getResource("/englishPCFG.ser.gz");
      lp =  new LexicalizedParser(url.toString());
    }

    @Override
    public void map(LongWritable key, Text val, Context context) throws
    IOException, InterruptedException{
      StringTokenizer itr = new StringTokenizer(val.toString());
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        if(lp.getLexicon().isKnown(token)){
          word.set(token);
          context.write(word, one);          
        }
      }
    }
  }



  public static class WordCountReducer extends
  Reducer<Text, IntWritable, Text, IntWritable>{

    private final static IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException{
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  
  public static class SimpleHashPartitioner implements Partitioner<Text, IntWritable>{

    @Override
    public int getPartition(Text word, IntWritable freq, int n) {
      return (word.toString().hashCode() & Integer.MAX_VALUE) % n;
    }
    
  }



  public static void main(String[] args) {
    
    args = new String[2];
    args[0] = "/home/dapurv5/Desktop/hdfs-input/literature";
    args[1] = "/home/dapurv5/Desktop/hdfs-output/wordcount";
    
    MapRedBSPJob job = new MapRedBSPJob();
    job.setJobName("Map Reduce BSP Job");
    job.setJarByClass(WordCount.class);
    
    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));
    
    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    job.setMapInputKeyClass(LongWritable.class);
    job.setMapInputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setReduceOutputKeyClass(Text.class);
    job.setReduceOutputValueClass(IntWritable.class);

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setPartitionerClass(SimpleHashPartitioner.class);
    
    try {
      job.waitForCompletion(true);

    } catch (IOException | InterruptedException | 
        ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}