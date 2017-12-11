package org.umn.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;
import org.umn.index.PointQ;
import org.umn.index.RectangleQ;

import com.org.json.JSONArray;
import com.org.json.JSONException;
import com.org.json.JSONObject;

public class TimeJobMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, IntWritable> {

	static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	final static String TWITTER = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
	static SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	private static RectangleQ[] mbrs = null;

	@Override
	public void configure(JobConf job) {
		Path pt = new Path(job.get("mbrFile"));
		readMBRFromDisk(pt, job);
	}

	@Override
	public void map(Object key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		try {
			String line = json2csv(value.toString());
			String[] tweet = line.split(",");
			if (tweet.length == 5) {
				String time = tweet[0];
				PointQ location = new PointQ(Double.parseDouble(tweet[2]),
						Double.parseDouble(tweet[1]));
				// get the information about the mbrs
				String rectangle = "";
				for (RectangleQ rect : mbrs) {
					if (rect.contains(location)) {
						rectangle = rect.x1 + "," + rect.y1 + "," + rect.x2
								+ "," + rect.y2;
						break;
					}
				}
				// check for the mbr that contains the tweet
				// check the keywords in the list
				word.set(rectangle + "\t" + time);
				output.collect(word, one);
				// System.out.println(word.toString());
			}
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void readMBRFromDisk(Path p, JobConf conf) {
		List<RectangleQ> result = new ArrayList<RectangleQ>();
		try {
			Path pt = p;
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;

			while ((line = br.readLine()) != null) {
				// System.out.println(line);
				String[] coordinates = line.split(",");
				RectangleQ rect = new RectangleQ(
						Double.parseDouble(coordinates[0]),
						Double.parseDouble(coordinates[1]),
						Double.parseDouble(coordinates[2]),
						Double.parseDouble(coordinates[3]));
				result.add(rect);
			}
			// copy to the mbrs of the map job.
			//result.toArray(mbrs);
			mbrs = new RectangleQ[result.size()];
			for (int i =0 ; i< result.size();i++){
				mbrs[i] = result.get(i);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static String json2csv(String jsonLine) throws ParseException {

		String csvOutput = "";
		double[] coords = new double[2];

		try {
			JSONObject post = new JSONObject(jsonLine);
			JSONObject user = post.getJSONObject("user");
			String created_at = fixDate(post.getString("created_at"));
			// String tweetID = post.getString("id_str");
			// String user_id = user.getString("id_str");
			// String user_screen_name = user.getString("screen_name");
			String tweetText = post.getString("text");
			String language = user.getString("lang").replace(",", ".");
			// String os = getOperatingSystem(post.getString("source"));
			String fixedTweetText = tweetText.replace('\n', ' ');
			String anotherFixedTweetText = fixedTweetText.replace(",", ".");
			String yetAnotherFixedTweeet = anotherFixedTweetText.replaceAll(
					"[\\t\\n\\r]", " ");
			// String followers_count = Integer.toString(user
			// .getInt("followers_count"));
			JSONObject geo = post.getJSONObject("geo");
			JSONArray coordsJSON = (JSONArray) geo.get("coordinates");
			coords[0] = coordsJSON.getDouble(0);
			coords[1] = coordsJSON.getDouble(1);

			// csvOutput = created_at + "," + tweetID + "," + user_id + ","
			// + user_screen_name + "," + yetAnotherFixedTweeet.trim()
			// + "," + followers_count + "," + language + "," + os + ","
			// + coords[0] + "," + coords[1] + "\n";
			csvOutput = created_at + "," + coords[0] + "," + coords[1] + ","
					+ language + "," + yetAnotherFixedTweeet.trim() + "\n";

			// csvOutput = created_at + "," + coords[0] + "," + coords[1] +
			// "\n";

		} catch (JSONException e) {
			csvOutput = "";
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return csvOutput;
	}

	

	public static Date getTwitterDate(String date) throws ParseException {
		sf.setLenient(true);

		return sf.parse(date);
	}

	public static String fixDate(String date) throws ParseException {
		Date dates = getTwitterDate(date);
		return sdf1.format(dates);
	}

	

}
