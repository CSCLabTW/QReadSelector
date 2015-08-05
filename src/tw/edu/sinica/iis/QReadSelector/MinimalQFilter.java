/*
 *  QReadSelector: subset selection of high-depth NGS reads for de novo assembly
 *  Copyright (C) 2015  The QReadSelector project, Academia Sinica, Taiwan.
 *  
 *  This file is part of QReadSelector.
 *
 *  QReadSelector is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/* 
 *  This program will cutoff reads that has minimal quality below a threshold (between 0 to 42).
 *  It takes the result of MinimalQ as its input, and produces fasta format output.
 *  
 *  Usage:  MinimalQFilter [result of MinimalQ] [output (fasta)] [threshold]
 */

package tw.edu.sinica.iis.QReadSelector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MinimalQFilter {
	final static boolean compress = true;
	final static int QBASE = 33;
	// number of reducer tasks
	final static int numOfReducers = 0;

	private class CODECS {
		private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
		private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";
	}

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			int cutoff = conf.getInt("cutoff", 1);
			Text result = new Text();

			String[] lineArray = value.toString().split("\t", 3);

			if (lineArray[2].charAt(0) > (cutoff + QBASE)) {
				/* output the result in fasta format */
				result.set(">" + lineArray[0] + "\n" + lineArray[1]);
				context.write(result, null);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("cutoff", args[2]);
		Job job = new Job(conf, "MinimalQFilter");
		job.setJarByClass(MinimalQFilter.class);

		if (compress) {
			// Hadoop 0.20 and before
			conf.setBoolean("mapred.compress.map.output", true);
			// Hadoop 0.21 and later
			conf.setBoolean("mapreduce.map.output.compress", true);

			if (conf.get("io.compression.codecs") != null) {
				if (conf.get("io.compression.codecs").contains(CODECS.SNAPPY)) {
					// Hadoop 0.20 and before
					conf.set("mapred.map.output.compression.codec", CODECS.SNAPPY);
					// Hadoop 0.21 and later
					conf.set("mapreduce.map.output.compress.codec", CODECS.SNAPPY);
				} else if (conf.get("io.compression.codecs").contains(CODECS.LZO)) {
					// Hadoop 0.20 and before
					conf.set("mapred.map.output.compression.codec", CODECS.LZO);
					// Hadoop 0.21 and later
					conf.set("mapreduce.map.output.compress.codec", CODECS.LZO);
				}
			}
		}
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(numOfReducers);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}