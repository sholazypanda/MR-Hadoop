package friendsAge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<CustomKey, Text> {
	@Override
	  public int getPartition(CustomKey maxAge,Text nullWritable, int numPartitions) {
	    return maxAge.getAge().hashCode() % numPartitions;
	}
}
