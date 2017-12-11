package org.umn.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortJob {

	/***
	 * 
	 * @author louai This class is the mapper Input of the mapper <MBR time
	 *         keyword count >
	 *
	 */
	public static class MbrMapper extends
			Mapper<Object, Text, Text, KeyValueItems> {
		private Text keys = new Text();
		private KeyValueItems values = new KeyValueItems();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] inputText = line.split("\t");
			if (inputText.length == 4) {
				String mbr = inputText[0];
				String time = inputText[1];
				String keyword = inputText[2];
				String count = inputText[3];
				keys.set(mbr + "\t" + time);
				values = new KeyValueItems(keyword, count);
				context.write(keys, values);
			}

		}
	}// end MbrMapper class

	/**
	 * 
	 * @author louai This is the reducer class The input of the reducer will be
	 *         Key, value Key -> MBR time Value -> <keyword,count> The output of
	 *         the reducer as following MBR \t time [\tkey,value]*
	 */
	public static class MbrReducer extends
			Reducer<Text, KeyValueItems, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<KeyValueItems> values,
				Context context) throws IOException, InterruptedException {

			List<String> list = new ArrayList<String>();
			StringBuilder temp = new StringBuilder();
			for (KeyValueItems val : values) {
				KeyValueItems obj = val;
				if ((obj.getKeyword().toString() != " " || obj.getKeyword()
						.toString() != "") && obj.getCount().get() != 0) {
					list.add(new KeyValueItems(obj).toString());
				}
			}
			List<KeyValueItems> sortedList = new ArrayList<KeyValueItems>();
			for (String stringItem : list) {
				String[] splited = stringItem.split(",");
				sortedList.add(new KeyValueItems(splited[0], splited[1]));
			}

			Collections.sort(sortedList);
			for (int index = 0; index < sortedList.size(); index++) {
				temp.append("\t" + sortedList.get(index).toString());
			}
			result.set(temp.toString());
			context.write(key, result);
		}
	}

}
