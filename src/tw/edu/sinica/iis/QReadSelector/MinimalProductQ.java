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
 *  This program will produce the minimalproductQ of each read using k-mer size K.
 *  It takes sfq format as its input.
 *  
 *  Usage:  MinimalRroductQ [input (sfq)] [output] [K]
 */

package tw.edu.sinica.iis.QReadSelector;

import java.io.IOException;
import java.util.HashMap;

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

public class MinimalProductQ {
	final static boolean compress = true;
	// number of reducer tasks
	final static int numOfReducers = 0;

	private class CODECS {
		private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
		private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";
	}

	static HashMap<Integer, Double> quality_p_array_ = initializeQualityPArray();

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			int k = conf.getInt("K", 1);
			Text result = new Text();

			String[] lineArray = value.toString().split("\t", 3);

			if (!lineArray[1].contains("N")) {
				double temp = 1.0;
				int minimal = 100;
				int length = lineArray[2].length();

				/*
				 * if read length is smaller than k, end =1 , otherwise end=
				 * L-k+1
				 */
				int end = ((length - k) <= 0) ? 1 : (length - k + 1);

				/*
				 * if read length is smaller than k, tempk=length, otherwise
				 * tempk=k
				 */
				int tempk = ((length - k) <= 0) ? length : (k);

				for (int i = 0; i < end; i++) {
					String l = ((length - k) <= 0) ? lineArray[2] : lineArray[2].substring(i, i + k);

					for (int j = 0; j < tempk; j++) {
						/* for each k-mer, calculate it's k-mer's productQ */
						temp *= quality_p_array_.get((((l.charAt(j)) - 33) <= 0) ? 0 : ((l.charAt(j)) - 33));
					}
					/* update the minimal ProductQ of read */
					if (minimal > ((int) (temp * 100))) {
						minimal = ((int) (temp * 100));
					}
					temp = 1.0;
				}

				result.set(lineArray[0] + "\t" + lineArray[1] + "\t" + minimal);

				context.write(result, null);

				minimal = 100;
			}

		}
	}

	/*
	 * hash table of the correctness probability of it's corresponding quality
	 * score
	 */
	private static HashMap<Integer, Double> initializeQualityPArray() {
		HashMap<Integer, Double> retval = new HashMap<Integer, Double>();

		for (int i = 0; i < 42; i++) {
			retval.put(i, 1 - Math.pow(10, -((double) i / 10)));
		}

		return retval;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("K", args[2]);

		Job job = new Job(conf, "MinimalProductQ");
		job.setJarByClass(MinimalProductQ.class);

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