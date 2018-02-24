package com.learning.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable totalcount = new IntWritable();
	// reduce method accepts Keyvalue pairs from Mappers & do the aggregation
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0; 
		Iterator<IntWritable> list=values.iterator();
		while(list.hasNext()) {	// iterates through all values with a key & adds it to result
			count+= list.next().get();
		}
			totalcount.set(count);
			context.write(key, totalcount);
		}
	}

