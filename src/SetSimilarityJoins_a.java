package SetSimilarityJoins;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;

class TextPair implements WritableComparable<TextPair> {

	private Text First;
	private Text Second;

	public TextPair(Text First, Text Second) {
		set(First, Second);
	}

	public TextPair() {
		set(new Text(), new Text());
	}

	public TextPair(String First, String Second) {
		set(new Text(First), new Text(Second));
	}

	public Text getFirst() {
		return First;
	}

	public Text getSecond() {
		return Second;
	}

	public void set(Text First, Text Second) {
		this.First = First;
		this.Second = Second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		First.readFields(in);
		Second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		First.write(out);
		Second.write(out);
	}

	@Override
	public String toString() {
		return First + " " + Second;
	}

	@Override
	public int compareTo(TextPair other) {
		int cmpFirstFirst = First.compareTo(other.First);
		int cmpSecondSecond = Second.compareTo(other.Second);
		int cmpFirstSecond = First.compareTo(other.Second);
		int cmpSecondFirst = Second.compareTo(other.First);

		if (cmpFirstFirst == 0 && cmpSecondSecond == 0 || cmpFirstSecond == 0
				&& cmpSecondFirst == 0) {
			return 0;
		}

		Text thisSmaller;
		Text otherSmaller;

		Text thisBigger;
		Text otherBigger;

		if (this.First.compareTo(this.Second) < 0) {
			thisSmaller = this.First;
			thisBigger = this.Second;
		} else {
			thisSmaller = this.Second;
			thisBigger = this.First;
		}

		if (other.First.compareTo(other.Second) < 0) {
			otherSmaller = other.First;
			otherBigger = other.Second;
		} else {
			otherSmaller = other.Second;
			otherBigger = other.First;
		}

		int cmpThisSmallerOtherSmaller = thisSmaller.compareTo(otherSmaller);
		int cmpThisBiggerOtherBigger = thisBigger.compareTo(otherBigger);

		if (cmpThisSmallerOtherSmaller == 0) {
			return cmpThisBiggerOtherBigger;
		} else {
			return cmpThisSmallerOtherSmaller;
		}
	}

	@Override
	public int hashCode() {
		return First.hashCode() * 163 + Second.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TextPair) {
			TextPair tp = (TextPair) o;
			return First.equals(tp.First) && Second.equals(tp.Second);
		}
		return false;
	}

}

public class SetSimilarityJoins_a extends Configured implements
		Tool {

	public static enum MyCounter {
		nComparaisons_a,
	};

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new SetSimilarityJoins_a(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = Job.getInstance(getConf(), "SetSimilarityJoins_a");

		job.setJarByClass(SetSimilarityJoins_a.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set(
				"mapreduce.input.keyvaluelinerecordreader.key.value.separator",
				",");
		job.getConfiguration().set(
				"mapreduce.output.textoutputformat.separator", ", ");
		job.setNumReduceTasks(1);

		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);
		long counter = job.getCounters()
				.findCounter(MyCounter.nComparaisons_a).getValue();
		Path outFile = new Path("nComparaisons_a.txt");
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				fs.create(outFile, true)));
		br.write(String.valueOf(counter));
		br.close();
		return 0;
	}

	public static class Map extends Mapper<Text, Text, TextPair, Text> {

		private BufferedReader reader;
		private static TextPair textPair = new TextPair();

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			HashMap<String, String> linesHP = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/SETSIMILARITY/output/noFreqPreProcessing.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] line = pattern.split(",");
				linesHP.put(line[0], line[1]);
			}

			for (String line : linesHP.keySet()) {
				if (key.toString().equals(line)) {
					continue;
				}

				textPair.set(key, new Text(line));
				context.write(textPair, new Text(value.toString()));
			}
		}
	}

	public static class Reduce extends Reducer<TextPair, Text, Text, Text> {

		private BufferedReader reader;

		public double jaccardsim(TreeSet<String> s1, TreeSet<String> s2) {

			if (s1.size() < s2.size()) {
				TreeSet<String> s1bis = s1;
				s1bis.retainAll(s2);
				int inter = s1bis.size();
				s1.addAll(s2);
				int union = s1.size();
				return (double) inter / union;
			} else {
				TreeSet<String> s1bis = s2;
				s1bis.retainAll(s1);
				int inter = s1bis.size();
				s2.addAll(s1);
				int union = s2.size();
				return (double) inter / union;
			}

		}

		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, String> linesHP = new HashMap<String, String>();
			reader = new BufferedReader(
					new FileReader(
							new File(
									"/home/cloudera/workspace/SETSIMILARITY/output/noFreqPreProcessing.txt")));
			String pattern;
			while ((pattern = reader.readLine()) != null) {
				String[] line = pattern.split(",");
				linesHP.put(line[0], line[1]);
			}

			TreeSet<String> wordsof2ndlineinpairTS = new TreeSet<String>();
			String wordsof2ndlineinpairS = linesHP.get(key.getSecond()
					.toString());
			for (String word : wordsof2ndlineinpairS.split(" ")) {
				wordsof2ndlineinpairTS.add(word);
			}

			TreeSet<String> wordsTS = new TreeSet<String>();

			for (String word : values.iterator().next().toString().split(" ")) {
				wordsTS.add(word);
			}

			context.getCounter(MyCounter.nComparaisons_a).increment(1);
			double sim = jaccardsim(wordsTS, wordsof2ndlineinpairTS);

			if (sim >= 0.8) {
				context.write(new Text("(" + key.getFirst() + ", " + key.getSecond() + ")"),
						new Text(String.valueOf(sim)));
			}
		}
	}
}
