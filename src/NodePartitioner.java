import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.Text;


public class NodePartitioner implements Partitioner<Text, Text>{

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		// partitioning key uniformly among all partitions
		return Integer.parseInt(key.toString()) % numPartitions;
	}

}
