package com.learning.mapper;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable (1);
	private Text word = new Text ();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer words = new StringTokenizer(line); // break line into words
		
		while(words.hasMoreTokens()) {	// iterating through all words in line & forming key value pair
		word.set(words.nextToken());
		context.write(word, one); // sending to output collector which would pass the word & 1 to reducer
		}
	}
}
