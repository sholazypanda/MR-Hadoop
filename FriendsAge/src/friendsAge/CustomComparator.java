package friendsAge;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class CustomComparator extends WritableComparator {

	  public CustomComparator() {
	    super(CustomKey.class, true);
	  }

	  @Override
	  public int compare(WritableComparable w1, WritableComparable w2) {
	    CustomKey key1 = (CustomKey) w1;
	    CustomKey key2 = (CustomKey) w2;

	    int cmpResult = -1*key1.getAge().compareTo(key2.getAge());
	    
	    
	    return cmpResult;
	  }
}
