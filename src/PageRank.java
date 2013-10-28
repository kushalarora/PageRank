import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {
	
	static String path = null;
	public static void setPaths(String outPath) {
		path = outPath;
	}
	public PageRank() {
		// TODO Auto-generated constructor stub
	}

	public static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Node node = null;
			String[] arr = key.toString().split(" ");
			assert (arr.length == 2);
			try {
				node = new Node(arr[0], Double.parseDouble(arr[1]),
						value.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			output.collect(new Text(node.getNodeId()), new Text(node
					.getCumulativeRank().toString() + "|" + node.getNodeId()));

			Double p = node.getCumulativeRank() / node.getList().size();

			Iterator<String> adjItr = node.getList().iterator();
			while (adjItr.hasNext()) {
				output.collect(new Text(adjItr.next()), new Text(p.toString()
						+ "|" + node.getNodeId()));
			}
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Node node = null;
			double s = 0.0;
			try {
				node = new Node(key.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				String[] arr = values.next().toString().split("|");
				assert (arr.length == 2);

				Double val = Double.parseDouble(arr[0]);
				if (node.getNodeId().compareTo(arr[1]) == 0) {
					node.setCumulativeRank(val);
				} else {
					s += val;
					sb.append(arr[1]);
					if (values.hasNext()) {
						sb.append(",");
					}
				}
			}
			node.setCumulativeRank(s);
			output.collect(new Text(node.toString()), new Text(sb.toString()));
		}

	}
	
	 private static void runJob(int runs) throws Exception {
         JobConf conf = new JobConf(PageRank.class);
         conf.setJobName("pagerank");

         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(Text.class);

         conf.setMapperClass(PageRank.Map.class);
         conf.setPartitionerClass(NodePartitioner.class);
         //conf.setCombinerClass(Reduce.class);
         conf.setReducerClass(PageRank.Reduce.class);
         
         conf.setInputFormat(KeyValueTextInputFormat.class);
         conf.setOutputFormat(TextOutputFormat.class);

         FileInputFormat.setInputPaths(conf, new Path(path + runs));
         FileOutputFormat.setOutputPath(conf, new Path(path + (runs+1)));
         
         JobClient.runJob(conf);
 }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		setPaths(args[0]);

	}

}
