package org.umn.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class KeyValueItems implements WritableComparable<KeyValueItems> {
	private Text keyword;
	private IntWritable count;

	public KeyValueItems(KeyValueItems object) {
		this.keyword = object.keyword;
		this.count = object.count;
	}

	public KeyValueItems() {
		this.keyword = new Text();
		this.count = new IntWritable(0);
	}
	
	public KeyValueItems(Text keyword, IntWritable count){
		this.keyword = keyword; 
		this.count = count;
	}

	public KeyValueItems(String keyword, String count) {
		this.keyword = new Text(keyword.toString().replace(',', '_').replace("\t", "_"));
		this.count = new IntWritable(Integer.parseInt(count));
	}

	public IntWritable getCount() {
		return count;
	}

	public Text getKeyword() {
		return keyword;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void setKeyword(Text keyword) {
		this.keyword = keyword;
	}

	@Override
	public int compareTo(KeyValueItems o) {
		// TODO Auto-generated method stub
		return o.count.get() - this.count.get();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		keyword.readFields(in);
		count.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		keyword.write(out);
		count.write(out);
	}

	@Override
	public String toString() {
		return keyword + "," + count;
	}

	public Text ToText() {
		return new Text(this.keyword.toString() + "," + this.count);
	}

}