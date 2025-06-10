package org.example.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DecimalFormat; // For formatting output if needed, though problem says unnecessary

public class TransactionCostReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private DoubleWritable result = new DoubleWritable();
  // private static final DecimalFormat df = new DecimalFormat("0.0"); // Optional formatting

  @Override
  protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
    double sum = 0;
    for (DoubleWritable val : values) {
      sum += val.get();
    }
    // result.set(Double.parseDouble(df.format(sum))); // Optional formatting
    result.set(sum);
    context.write(key, result);
  }
}