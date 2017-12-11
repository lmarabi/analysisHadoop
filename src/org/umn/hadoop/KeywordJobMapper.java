package org.umn.hadoop;

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

public class KeywordJobMapper extends MapReduceBase implements
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
				String language = tweet[tweet.length - 2];
				String before = tweet[tweet.length - 1];
				String after = removeStopWords(before, language);
				// check the keywords in the list
				StringTokenizer itr = new StringTokenizer(after);
				while (itr.hasMoreTokens()) {
					word.set(rectangle + "\t" + time + "\t" + itr.nextToken());
					output.collect(word, one);
				}
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

	public static String removeStopWords(String textFile, String language)
			throws Exception {
		CharArraySet stopWords = null;

		switch (language) {
		case "en":// english
			stopWords = EnglishAnalyzer.getDefaultStopSet();
			break;
		case "ar":// Arabic
			stopWords = ArabicAnalyzer.getDefaultStopSet();
			break;
		case "bg":// Bulgarian
			stopWords = BulgarianAnalyzer.getDefaultStopSet();
			break;
		case "br":// Brazilian
			stopWords = BrazilianAnalyzer.getDefaultStopSet();
			break;
		case "ca":// Catalan
			stopWords = CatalanAnalyzer.getDefaultStopSet();
			break;
		case "cn":// Chinese
			stopWords = CJKAnalyzer.getDefaultStopSet();
			break;
		case "cjk":// Chinese, Japanese, and Korean
			stopWords = CJKAnalyzer.getDefaultStopSet();
			break;
		case "cz":// Czech
			stopWords = CzechAnalyzer.getDefaultStopSet();
			break;
		case "da":// Danish
			stopWords = DanishAnalyzer.getDefaultStopSet();
			break;
		case "de":// German
			stopWords = GermanAnalyzer.getDefaultStopSet();
			break;
		case "el":// Greek
			stopWords = GreekAnalyzer.getDefaultStopSet();
			break;
		case "es":// Spanish
			stopWords = SpanishAnalyzer.getDefaultStopSet();
			break;
		case "eu":// Basque
			stopWords = BasqueAnalyzer.getDefaultStopSet();
			break;
		case "fa":// Persian
			stopWords = PersianAnalyzer.getDefaultStopSet();
			break;
		case "fi":// Finnish
			stopWords = FinnishAnalyzer.getDefaultStopSet();
			break;
		case "fr":// French
			stopWords = FrenchAnalyzer.getDefaultStopSet();
			break;
		case "ga":// Irish
			stopWords = IrishAnalyzer.getDefaultStopSet();
			break;
		case "gl":// Galician
			stopWords = GalicianAnalyzer.getDefaultStopSet();
			break;
		case "hi":// Hindi
			stopWords = HindiAnalyzer.getDefaultStopSet();
			break;
		case "hu":// Hungarian
			stopWords = HungarianAnalyzer.getDefaultStopSet();
			break;
		case "hy":// Armenian
			stopWords = ArmenianAnalyzer.getDefaultStopSet();
			break;
		case "id":// Indonesian
			stopWords = IndonesianAnalyzer.getDefaultStopSet();
			break;
		case "it":// Italian
			stopWords = ItalianAnalyzer.getDefaultStopSet();
			break;
		case "lv":// Latvian
			stopWords = LatvianAnalyzer.getDefaultStopSet();
			break;
		case "nl":// Dutch
			stopWords = DutchAnalyzer.getDefaultStopSet();
			break;
		case "no":// Norwegian
			stopWords = NorwegianAnalyzer.getDefaultStopSet();
			break;
		case "pt":// Portuguese
			stopWords = PortugueseAnalyzer.getDefaultStopSet();
			break;
		case "ro":// Romanian
			stopWords = RomanianAnalyzer.getDefaultStopSet();
			break;
		case "ru":// Russian
			stopWords = RussianAnalyzer.getDefaultStopSet();
			break;
		case "sv":// Russian
			stopWords = SwedishAnalyzer.getDefaultStopSet();
			break;
		case "th":// Thai
			stopWords = ThaiAnalyzer.getDefaultStopSet();
			break;
		case "tr":// Turkish
			stopWords = TurkishAnalyzer.getDefaultStopSet();
			break;
		default:
			stopWords = EnglishAnalyzer.getDefaultStopSet();

		}
		return StopwordsFilter(textFile, stopWords);
	}

	private static String StopwordsFilter(String textFile,
			CharArraySet stopWords) throws IOException {
		TokenStream tokenStream = new StandardTokenizer(Version.LUCENE_46,
				new StringReader(removeLinksFromString(textFile).trim()));

		tokenStream = new StopFilter(Version.LUCENE_46, tokenStream, stopWords);
		StringBuilder sb = new StringBuilder();
		CharTermAttribute charTermAttribute = tokenStream
				.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		while (tokenStream.incrementToken()) {
			String term = charTermAttribute.toString();
			if (!containsDigit(term)) {
				sb.append(term + " ");
			}
		}
		return sb.toString();
	}

	public static String removeLinksFromString(String s) {
		StringBuilder result = new StringBuilder();
		String regex = "\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
		String regex2 = "@\\w+";
		String nosiePattern1 = "[A|S|D|G|Q|W|E|R|T|Y|U|I|O|P|Z|X|C|V|B|N|M|<|>|?|\"|:|L|K|J|H|}|{|?|~|!|@|#|%|^|&|*|(|)|_|+|=|-]+";
		String nosiePattern2 = "[a-zA-Z]";
		String nosiePattern3 = "(\"|&|~|@|#|$|%|^|\\(|\\)|\\+|-)+\\w+";
		String nosiePattern4 = "((\\s?|\\s+)\\w\\s+)+";

		StringTokenizer itr = new StringTokenizer(s);
		while (itr.hasMoreTokens()) {
			String temp = itr.nextToken().toString();
			if (!temp.matches(regex) && !temp.matches(regex2)
					&& !temp.matches(nosiePattern1)
					&& !temp.matches(nosiePattern2) && !containsDigit(temp)
					&& !temp.matches(nosiePattern3)
					&& !temp.matches(nosiePattern4) && temp.length() > 2) {
				result.append(temp + " ");
			}
		}
		return result.toString();
	}

	public static boolean containsDigit(String s) {
		boolean containsDigit = false;

		if (s != null && !s.isEmpty()) {
			for (char c : s.toCharArray()) {
				if (containsDigit = Character.isDigit(c)) {
					break;
				}
			}
		}

		return containsDigit;
	}

	public static Date getTwitterDate(String date) throws ParseException {
		sf.setLenient(true);

		return sf.parse(date);
	}

	public static String fixDate(String date) throws ParseException {
		Date dates = getTwitterDate(date);
		return sdf1.format(dates);
	}

	public static String getOperatingSystem(String os) {
		String result = "";
		if (os.contains("BlackBerry")) {
			result = "BlackBerry";
		} else if (os.contains("Android")) {
			result = "Android";
		} else if (os.contains("Mac")) {
			result = "Mac";
		} else if (os.contains("iPhone")) {
			result = "iPhone";
		} else if (os.contains("Nokia")) {
			result = "Nokia";
		} else if (os.contains("Instagram")) {
			result = "Instagram";
		} else if (os.contains("iOS")) {
			result = "iOS";
		} else if (os.contains("FourSquare")) {
			result = "FourSquare";
		} else if (os.contains("Windows")) {
			result = "Windows";
		} else if (os.contains("iPad")) {
			result = "iPad";
		} else {
			result = "Other";
		}
		return result;
	}

}
