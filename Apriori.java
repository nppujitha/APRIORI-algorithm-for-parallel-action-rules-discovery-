package com.part3.group5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Apriori {
	public static ArrayList<String> attributeList = new ArrayList();
	public static ArrayList<String> dataFileList = new ArrayList();
	public static ArrayList<String> originalAttributes = new ArrayList();
	public static ArrayList<String> stableAttributes = new ArrayList();
	public static ArrayList<String> decisionAttributes = new ArrayList();
	public static ArrayList<String> decisionFromAndTo = new ArrayList();
	public static String decisionFrom = new String();
	public static String decisionTo = new String();
	public static int minSupport = 0;
	public static int minConfidence = 0;
	public static Scanner input = new Scanner(System.in);
	public static int index = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		File attribute = new File(args[0]);
		FileReader attribute_reader = new FileReader(attribute);
		BufferedReader attribute_buffer = new BufferedReader(attribute_reader);
		String att = new String();

		while ((att = attribute_buffer.readLine()) != null) {
			attributeList.addAll(Arrays.asList(att.split("\\s+")));
			originalAttributes.addAll(Arrays.asList(att.split("\\s+")));
		}

		int count = 0;
		attribute_reader.close();
		attribute_buffer.close();
		File data = new File(args[1]);
		FileReader data_reader = new FileReader(data);
		BufferedReader data_buffer = new BufferedReader(data_reader);
		String line = new String();

		while ((line = data_buffer.readLine()) != null) {
			count++;
		}

		data_reader.close();
		data_buffer.close();
		
		// Call functions to get user input
		setStableAttributes();
		setDecisionAttribute();
		setDecisionFromAndTo(args[1]);

		// User enters Support & Confidence input
		System.out.println("Please enter minimum Support: ");
		minSupport = input.nextInt();

		System.out.println("Please enter minimum Confidence in %: ");
		minConfidence = input.nextInt();
		input.close();

		Configuration configuration = new Configuration();

		configuration.set("mapreduce.input.fileinputformat.split.mazsize", data.length() / 5L + "");
		configuration.set("mapreduce.input.fileinputformat.split.minsize", "0");

		configuration.setInt("count", count);
		configuration.setStrings("attributes", (String[]) Arrays.copyOf(originalAttributes.toArray(),
				originalAttributes.toArray().length, String[].class));

		configuration.setStrings("stable", (String[]) Arrays.copyOf(stableAttributes.toArray(),
				stableAttributes.toArray().length, String[].class));

		configuration.setStrings("decision", (String[]) Arrays.copyOf(decisionAttributes.toArray(),
				decisionAttributes.toArray().length, String[].class));

		configuration.set("decisionFrom", decisionFrom);
		configuration.set("decisionTo", decisionTo);
		configuration.set("support", minSupport + "");
		configuration.set("confidence", minConfidence + "");

		Job actionRulesJob = Job.getInstance(configuration);

		actionRulesJob.setJarByClass(ActionRulesProg.class);

		actionRulesJob.setMapperClass(ActionRulesProg.JobMapper.class);
		actionRulesJob.setReducerClass(ActionRulesProg.JobReducer.class);

		actionRulesJob.setNumReduceTasks(1);

		actionRulesJob.setOutputKeyClass(Text.class);
		actionRulesJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(actionRulesJob, new Path(args[1]));

		FileOutputFormat.setOutputPath(actionRulesJob, new Path(args[2]));

		actionRulesJob.waitForCompletion(true);

		Job associationActionRulesJob = Job.getInstance(configuration);

		associationActionRulesJob.setJarByClass(AscActionRules.class);

		associationActionRulesJob.setMapperClass(AscActionRules.JobMapper.class);

		associationActionRulesJob.setReducerClass(AscActionRules.JobReducer.class);

		associationActionRulesJob.setNumReduceTasks(1);

		associationActionRulesJob.setOutputKeyClass(Text.class);
		associationActionRulesJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(associationActionRulesJob, new Path(args[1]));

		FileOutputFormat.setOutputPath(associationActionRulesJob, new Path(args[3]));

		associationActionRulesJob.waitForCompletion(true);
	}

	public static void setStableAttributes() { // Select Stable attributes
		boolean flag = false;
		String[] stable = null;
		System.out.println("****************Apriori_Algorithm_Output****************");
		System.out.println("List of Available Attributes: " + attributeList.toString());

		System.out.println("Please Enter The Stable Attribute(s):");
		String s = input.next();
		// Validate user input for Stable attributes
		if (s.split(",").length > 1) {
			stable = s.split(",");
			for (int j = 0; j < stable.length; j++) {
				if (!attributeList.contains(stable[j])) {
					System.out.println("Invalid Stable Attribute(s)");
					flag = true;
					break;
				}
			}
			if (!flag) {
				stableAttributes.addAll(Arrays.asList(stable));
				attributeList.removeAll(stableAttributes);
			}
		} else if (!attributeList.contains(s)) {
			System.out.println("Invalid Stable Attribute(s)");
		} else {
			stableAttributes.add(s);
			attributeList.removeAll(stableAttributes);
		}
		System.out.println("Stable Attribute(s): " + stableAttributes.toString());

		System.out.println("Available Attribute(s): " + attributeList.toString());
	}

	public static void setDecisionAttribute() { // Set Decision attribute
		System.out.println("1. Enter The Decision Attribute:");
		String s = input.next();
		if (!attributeList.contains(s)) {
			System.out.println("Invalid Decision Attribute(s)");
		} else {
			decisionAttributes.add(s);
			index = originalAttributes.indexOf(s);
			attributeList.removeAll(decisionAttributes);
		}
	}

	public static void setDecisionFromAndTo(String args) { // Set Decision FROM and TO attributes
		HashSet<String> set = new HashSet();
		File data = new File(args);

		// Read data file
		try {
			FileReader fileReader = new FileReader(data);
			BufferedReader data_buffer = new BufferedReader(fileReader);
			String str = new String();
			while ((str = data_buffer.readLine()) != null) {
				set.add(str.split(",")[index]);
			}
			fileReader.close();
			data_buffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println();
		Iterator<String> iterator = set.iterator();

		while (iterator.hasNext()) {
			decisionFromAndTo.add((String) originalAttributes.get(index) + (String) iterator.next());
		}

		// Ask user for input regarding Decision attribute
		System.out.println("Available Decision Attributes Are: " + decisionFromAndTo.toString());
		System.out.println("Enter Decision 'FROM' Attribute: ");
		decisionFrom = input.next();

		System.out.println("Enter Decision 'TO' Attribute: ");
		decisionTo = input.next();

		// Print summary
		System.out.println("Stable Attributes Are: " + stableAttributes.toString());

		System.out.println("Decision Attribute Is: " + decisionAttributes.toString());

		System.out.println("Decision FROM: " + decisionFrom);
		System.out.println("Decision TO: " + decisionTo);

		System.out.println("Flexible Attribute(s) Are: " + attributeList.toString());
	}
}
