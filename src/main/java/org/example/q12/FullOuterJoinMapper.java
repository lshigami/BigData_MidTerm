package org.example.q12;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FullOuterJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
  private Text outKey = new Text();   // common key (e.g., Pizza)
  private Text outValue = new Text(); // tagged value (e.g., "P:8" or "Q:3")

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] parts = line.split("\\s+", 3);

    if (parts.length == 3) {
      String tableName = parts[0];
      String commonKey = parts[1];
      String val = parts[2];

      outKey.set(commonKey);
      if ("FoodPrice".equals(tableName)) {
        outValue.set("P:" + val); // Tag for Price
      } else if ("FoodQuantity".equals(tableName)) {
        outValue.set("Q:" + val); // Tag for Quantity
      } else {
        return; // Ignore other tables if any
      }
      context.write(outKey, outValue);
    }
  }
}
