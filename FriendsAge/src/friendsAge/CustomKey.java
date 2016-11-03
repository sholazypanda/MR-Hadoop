package friendsAge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey> {
	  private Integer userId;
	  private Integer age;

	  public Integer getUserId() {
	    return userId;
	  }

	  public void setUserId(Integer userId) {
	    this.userId = userId;
	  }

	  public Integer getAge() {
	    return age;
	  }

	  public void setAge(Integer age) {
	    this.age = age;
	  }

	public CustomKey(Integer user, Integer age) {
	  // TODO Auto-generated constructor stub
	    this.userId=user;
	    this.age=age;
	}

	  public CustomKey(){}

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    userId = in.readInt();
	    age = in.readInt();
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(userId);
	    out.writeInt(age);
	  }

	  @Override
	  public int compareTo(CustomKey o) {
	    // TODO Auto-generated method stub

	    int result = userId.compareTo(o.userId);
	    if (result != 0) {
	      return result;
	    }
	    return this.age.compareTo(o.age);

	  }

	  @Override
	  public String toString() {
	    return "<" + userId.toString() + "," + age.toString()+">";
	  }

	  @Override
	  public boolean equals(Object obj) {
	    if (obj == null) {
	      return false;
	    }
	    if (getClass() != obj.getClass()) {
	      return false;
	    }
	    final CustomKey other = (CustomKey) obj;
	    if (this.userId != other.userId
	        && (this.userId == null || !this.userId.equals(other.userId))) {
	      return false;
	    }
	    if (this.age != other.age
	        && (this.age == null || !this.age.equals(other.age))) {
	      return false;
	    }
	    return true;
	  }
}
