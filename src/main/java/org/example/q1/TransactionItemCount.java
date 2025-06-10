package org.example.q1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TransactionItemCount {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: TransactionItemCount <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Transaction Item Count Q1");

    job.setJarByClass(TransactionItemCount.class);
    job.setMapperClass(TransactionItemCountMapper.class);
    job.setReducerClass(TransactionItemCountReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

