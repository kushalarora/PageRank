import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class KeyValueParser {
	static String inputPath = null;
	static String outputPath = null;
	public static void setPaths(String inpPath, String outPath) {
		inputPath = inpPath;
		outputPath = outPath;
	}
		public static class Map extends MapReduceBase implements
				Mapper<LongWritable, Text, Text, Text> {

			public void map(LongWritable key, Text value,
					OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
				String line = value.toString();
				String[] results = line.split(" "); // split text on spaces to
														// create words.
				if (results.length < 1) {
					new Exception("No node found");
				}
				// The graph node. 
				// Each node while processing page rank is of the format
				// (nodeId pageRank)
				Text node = new Text(results[0].trim() + " 1.00");
				for (int i = 1; i < results.length; i++) {
					String result = results[i].trim();
					if (result.length() > 0) {
						output.collect(node, new Text(result));

					}
				}
			}

		}

		public static class Reduce extends MapReduceBase implements
				Reducer<Text, Text, Text, Text> {
			public void reduce(Text key, Iterator<Text> values,
					OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
				StringBuilder sb = new StringBuilder();
				while (values.hasNext()) {
					sb.append(values.next().toString());
					if (values.hasNext())
						sb.append(",");
				}
			}

		}
	

	/**
	 * @param args
	 * @throws Exception 
	 */
	
		public static void runJob() throws Exception {
			assert (inputPath != null && outputPath != null);
			JobConf conf = new JobConf(KeyValueParser.class);
			conf.setJobName("keyvalues");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			conf.setMapperClass(KeyValueParser.Map.class);
			conf.setCombinerClass(KeyValueParser.Reduce.class);
			conf.setReducerClass(KeyValueParser.Reduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(inputPath));
			FileOutputFormat.setOutputPath(conf, new Path(outputPath + "0"));
			JobClient.runJob(conf);
		}
		
		/**
		 * @param args
		 */
		public static void main(String[] args) {
			KeyValueParser.setPaths(args[0], args[1]);
			try {
				KeyValueParser.runJob();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}

