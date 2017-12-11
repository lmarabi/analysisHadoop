package org.umn.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.umn.hadoop.KeyValueItems;

public class SortTimeJob {

	/***
	 * 
	 * @author louai This class is the mapper Input of the mapper <MBR time
	 *          count >
	 *
	 */
	public static class SortTimeMapper extends
			Mapper<Object, Text, Text, KeyValueItems> {
		private Text keys = new Text();
		private KeyValueItems values = new KeyValueItems();

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] inputText = line.split("\t");
			if (inputText.length == 3) {
				String mbr = inputText[0];
				String time = inputText[1];
				String count = inputText[2];
				keys.set(mbr);
				values = new KeyValueItems(time, count);
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
	public static class SortTimeReducer extends
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
			for (int index = 0; index < sortedList.size(); index++) {
				temp.append("\t" + sortedList.get(index).toString());
			}
			result.set(temp.toString());
			context.write(key, result);
		}
	}

}
