package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        String line = value.toString();
        String year = line.substring(15, 19);
        int airtemperature = Integer.valueOf(line.substring(87, 92));
        String quality = line.substring(92, 93);
        if (airtemperature != MISSING && quality.matches("[01459]"))
        {
            context.write(new Text(year), new IntWritable(airtemperature));
        }

    }
}
