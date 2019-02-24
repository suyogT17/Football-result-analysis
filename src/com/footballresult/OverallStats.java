package com.footballresult;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.footballresult.AwayGoals.IntSumReducer;
import com.footballresult.AwayGoals.TokenizerMapper;

public class OverallStats {

	public static class AwayGoalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text result = new Text("Away Goals: ");
		private int sum = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String team = config.get("mapper.team");
			if (key.toString().equals(team)) {
				for (IntWritable val : values) {
					sum += val.get();
				}
			}

			/*
			 * if(sum > freq){ freq = sum; result.set(key); } // context.write(key,new
			 * IntWritable(sum)) ;
			 */
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(result, new IntWritable(sum));
		}

	}

	public static class HomeGoalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text result = new Text("Home Goals: ");
		private int sum = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			String team = config.get("mapper.team");
			if (key.toString().equals(team)) {

				for (IntWritable val : values) {
					sum += val.get();
				}
			}

			/*
			 * if(sum > freq){ freq = sum; result.set(key); } // context.write(key,new
			 * IntWritable(sum)) ;
			 */
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(result, new IntWritable(sum));

		}

	}

	public static class GoalsConcededReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text result = new Text("Goals Conceded: ");
		private int sum = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			String team = config.get("mapper.team");
			if (key.toString().equals(team)) {

				for (IntWritable val : values) {
					sum += val.get();
				}
			}

			/*
			 * if(sum > freq){ freq = sum; result.set(key); } // context.write(key,new
			 * IntWritable(sum)) ;
			 */
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(result, new IntWritable(sum));

		}

	}

	public static class GoalsScoredReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text result = new Text("Goals Scored");
		private int sum = 0;

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String team = config.get("mapper.team");
			if (key.toString().equals(team)) {

				for (IntWritable val : values) {
					sum += val.get();
				}
			}

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			context.write(result, new IntWritable(sum));
		}

	}

	public static class WinPercentReducer extends Reducer<Text, Text, Text, Text> {
		private Text lose = new Text("Match Lose: ");
		private Text loseper = new Text("Losing Percentage: ");
		private Text won = new Text("Match Won: ");
		private Text wonper = new Text("Winning Percentage: ");
		private Text matchplayed = new Text("Match Played: ");
		private Text tie = new Text("Match Tie: ");
		private Text tieper = new Text("Tie Percentage: ");

		private int mwon = 0;
		private int mlose = 0;
		private int mtie = 0;
		int count = 0;
		String team;
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Configuration config = context.getConfiguration();
			team = config.get("mapper.team");
			
			if (key.toString().equals(team)) {

				for (Text val : values) {
					count++;
					if (val.toString().equals("W")) {
						mwon++;
					}
					if (val.toString().equals("T")) {
						mtie++;
					}
					if (val.toString().equals("L")) {
						mlose++;
					}
				}
			}

			/*
			 * if(sum > freq){ freq = sum; result.set(key); } // context.write(key,new
			 * IntWritable(sum)) ;
			 */
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			float wonpercent = (float) ((float) mwon / (float) count) * 100;
			float losepercent = (float) ((float) mlose / (float) count) * 100;
			float tiepercent = (float) ((float) mtie / (float) count) * 100;
			
			context.write(new Text("Team: "), new Text(team));

			context.write(matchplayed, new Text(count + ""));
			context.write(won, new Text(mwon + ""));

			context.write(wonper, new Text(wonpercent + "%"));
			context.write(lose, new Text(mlose + ""));
			context.write(loseper, new Text(losepercent + "%"));
			context.write(tie, new Text(mtie + ""));
			context.write(tieper, new Text(tiepercent + "%"));

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapper.team", args[2]);

		Job job = Job.getInstance(conf, "Winpercent");
		job.setJarByClass(OverallStats.class);
		job.setMapperClass(WinPercent.TokenizerMapper.class);
		job.setReducerClass(WinPercentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path inp = new Path(args[0]);
		Path out1 = new Path("allstats/winpercent");
		

		FileInputFormat.addInputPath(job, inp);
		FileOutputFormat.setOutputPath(job, out1);
		job.waitForCompletion(true);
		
	    Job job1 = Job.getInstance(conf, "GoalScored");
	    job1.setJarByClass(OverallStats.class);
	    job1.setMapperClass(GoalScored.TokenizerMapper.class);
	    job1.setReducerClass(GoalsScoredReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		Path out2 = new Path("allstats/goalscored");

		
	    FileInputFormat.addInputPath(job1, inp);
	    FileOutputFormat.setOutputPath(job1, out2);
		job1.waitForCompletion(true);


	    Job job2 = Job.getInstance(conf, "Home Goals");
	    job2.setJarByClass(OverallStats.class);
	    job2.setMapperClass(HomeGoals.TokenizerMapper.class);
	    job2.setReducerClass(HomeGoalReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		Path out3 = new Path("allstats/homegoals");

	   
	    FileInputFormat.addInputPath(job2, inp);
	    FileOutputFormat.setOutputPath(job2, out3);
		job2.waitForCompletion(true);


	    Job job3 = Job.getInstance(conf, "Away Goals");
	    job3.setJarByClass(OverallStats.class);
	    job3.setMapperClass(AwayGoals.TokenizerMapper.class);
	    job3.setReducerClass(AwayGoalReducer.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(IntWritable.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		Path out4 = new Path("allstats/awaygoals");

	   
	    FileInputFormat.addInputPath(job3, inp);
	    FileOutputFormat.setOutputPath(job3, out4);
		job3.waitForCompletion(true);

		
	    Job job4 = Job.getInstance(conf, "Goals Conceded");
	    job4.setJarByClass(OverallStats.class);
	    job4.setMapperClass(GoalsConceded.TokenizerMapper.class);
	    job4.setReducerClass(GoalsConcededReducer.class);
	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(IntWritable.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		Path out5 = new Path("allstats/goalsconceded");

	   
	    FileInputFormat.addInputPath(job4, inp);
	    FileOutputFormat.setOutputPath(job4, out5);
		
		
		job4.waitForCompletion(true);
		
		Process p=Runtime.getRuntime().exec("hadoop fs -getmerge allstats/winpercent/part-r-* allstats/goalscored/part-r-*"
				+ " allstats/homegoals/part-r-* allstats/awaygoals/part-r-* allstats/goalsconceded/part-r-* "+args[1]);
		
		p.waitFor();
		if(p.exitValue()==0) {
			System.exit(0);
		}
	}

}
