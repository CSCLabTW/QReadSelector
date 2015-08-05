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
 *  This program generates quality statistics for each base, or for minimal quality of each read.
 *  It accepts two types of input, sfq or the result of MinimalQ.
 *  
 *  Usage:  Qstatistics [input] [output]
 */

package tw.edu.sinica.iis.QReadSelector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Qstatistics {
	final static boolean compress = true;
	// number of reducer tasks
	final static int numOfReducers = 70;

	private class CODECS {
		private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
		private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";
	}

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text Q = new Text();

		int index = 0;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			long ReadQ[] = new long[42];
			for (int i = 0; i < 42; i++) {
				ReadQ[i] = 0;
			}

			String[] lineArray = value.toString().split("\t", 3);
			// String ID = lineArray[0];
			String Seq = lineArray[1];
			String Qstr = lineArray[2];

			if (!Seq.contains("N")) {
				/* for each read, count the number of quality score */
				for (int i = 0; i < Qstr.length(); i++) {
					index = (((Qstr.charAt(i)) - 33) <= 0) ? 0 : ((Qstr.charAt(i)) - 33);
					ReadQ[index] += 1;
				}

				for (int i = 0; i < 42; i++) {
					LongWritable Freq = new LongWritable(ReadQ[i]);
					Q.set(Long.toString(i));
					context.write(Q, Freq);
				}
			}

		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			Long sum = (long) 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Qstatistics");
		job.setJarByClass(Qstatistics.class);
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
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(numOfReducers);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}