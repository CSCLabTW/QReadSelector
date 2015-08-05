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
 *  This program will produce the minimal quality score of each pair of reads.
 *  It takes shuffled fastq format as its input.
 *  
 *  Usage:  PEMinimalQ [input (fastq)] [output]
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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PEMinimalQ {
	static int count = 0;
	static String str1 = null;
	static String Qstr1 = null;
	static String id2[] = null;
	static String str2 = null;
	static String Qstr2 = null;
	static String id1[] = null;
	static char minimal1 = 'J';
	static char minimal2 = 'J';
	static int threshold = -1;
	final static boolean compress = true;
	// number of reducer tasks
	final static int numOfReducers = 0;

	private class CODECS {
		private static final String SNAPPY = "org.apache.hadoop.io.compress.SnappyCodec";
		private static final String LZO = "com.hadoop.compression.lzo.LzoCodec";
	}

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Text result = new Text();
			/*
			 * since the input is shuffled fastq file, mapper reads 8 lines as
			 * an unit. line 1: id1 line 2: Read1 line 3: + line 4: Qstr1 line
			 * 5: id2 line 6: Read2 line 7: + line 8: Qstr2
			 */
			switch (count) {
			case 0:
				id1 = value.toString().split(" ", 2);
				count++;
				break;
			case 1:
				str1 = value.toString();
				count++;
				break;
			case 2:
				count++;
				break;
			case 3:
				Qstr1 = value.toString();
				minimal1 = 'J';
				int QstrLen1 = Qstr1.length();

				if (!str1.contains("N")) {
					for (int i = 0; i < QstrLen1; i++) {
						minimal1 = (minimal1 < Qstr1.charAt(i)) ? minimal1 : Qstr1.charAt(i);
					}
				}
				count++;
				break;
			case 4:
				id2 = value.toString().split(" ", 2);
				count++;
				break;
			case 5:
				str2 = value.toString();
				count++;
				break;
			case 6:
				count++;
				break;
			case 7:
				Qstr2 = value.toString();
				String device1 = id1[0];
				String device2 = id2[0];
				minimal2 = 'J';
				int QstrLen2 = Qstr2.length();

				if ((!str1.contains("N")) && (!str2.contains("N"))) {
					/*
					 * verify that Read1 is pair with Read2 using it's
					 * corresponding id information
					 */
					if (device1.equals(device2)) {
						for (int i = 0; i < QstrLen2; i++) {
							minimal2 = (minimal2 < Qstr2.charAt(i)) ? minimal2 : Qstr2.charAt(i);
						}
						result.set(id1[0] + " " + id1[1] + "\t" + str1 + "\t" + minimal1 + "\t" + id2[0] + " " + id2[1]
								+ "\t" + str2 + "\t" + minimal2);
						context.write(result, null);
					}
				}
				count = 0;
				break;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PEMinimalQ");
		job.setJarByClass(PEMinimalQ.class);

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

		NLineInputFormat.setNumLinesPerSplit(job, 100000);

		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(numOfReducers);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
