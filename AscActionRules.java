package com.part3.group5;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AscActionRules {

	public static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
		int lineCount = 0;
		double minSupport;
		double minConfidence;
		String decisionAttribute;
		String decisionFromAttribute;
		String decisionToAttribute;
		public static ArrayList<String> attributeNamesList;
		public static ArrayList<String> stableAttributesList;
		public static ArrayList<String> stableAttributeValuesList;
		public static ArrayList<String> decisionFromAndToList;
		ArrayList<HashSet<String>> associations;
		static Map<ArrayList<String>, Integer> dataMap;
		static Map<String, HashSet<String>> distinctAttributeMap;
		static Map<String, HashSet<String>> decisionValuesMap;
		static Map<HashSet<String>, HashSet<String>> attributeValues;
		Map<ArrayList<ArrayList<String>>, ArrayList<String>> actionMap;
		Map<ArrayList<ArrayList<String>>, ArrayList<String>> derivedActionMap;
		Map<String, ArrayList<ArrayList<String>>> singleSet;

		public JobMapper() {
			attributeNamesList = new ArrayList();
			stableAttributesList = new ArrayList();
			stableAttributeValuesList = new ArrayList();
			decisionFromAndToList = new ArrayList();
			this.associations = new ArrayList();

			dataMap = new HashMap();
			distinctAttributeMap = new HashMap();
			decisionValuesMap = new HashMap();
			attributeValues = new HashMap();
			this.actionMap = new HashMap();
			this.derivedActionMap = new HashMap();
			this.singleSet = new HashMap();
		}

		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			attributeNamesList = new ArrayList(Arrays.asList(context.getConfiguration().getStrings("attributes")));

			stableAttributesList = new ArrayList(Arrays.asList(context.getConfiguration().getStrings("stable")));

			this.decisionAttribute = context.getConfiguration().getStrings("decision")[0];

			this.decisionFromAttribute = context.getConfiguration().get("decisionFrom");
			this.decisionToAttribute = context.getConfiguration().get("decisionTo");
			decisionFromAndToList.add(this.decisionFromAttribute);
			decisionFromAndToList.add(this.decisionToAttribute);
			
			this.minSupport = Double.parseDouble(context.getConfiguration().get("support"));

			this.minConfidence = Double.parseDouble(context.getConfiguration().get("confidence"));

			super.setup(context);
		}

		protected void map(LongWritable key, Text inputValue, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			splitData(inputValue, this.lineCount);
			for (int i = 0; i < stableAttributesList.size(); i++) {
				HashSet<String> distinctStableValues = (HashSet) distinctAttributeMap.get(stableAttributesList.get(i));
				for (String string : distinctStableValues) {
					if (!stableAttributeValuesList.contains(string)) {
						stableAttributeValuesList.add(string);
					}
				}
			}
			setDecisionAttributeValues();

			this.lineCount += 1;
		}

		private void splitData(Text inputValue, int lineCount) {
			int lineNo = lineCount;

			String inputData = inputValue.toString();
			ArrayList<String> lineData = new ArrayList(Arrays.asList(inputData.split("\t|,")));
			if (!checkEmptyValueInStringArray(lineData)) {
				lineNo++;

				ArrayList<String> tempList = new ArrayList();
				for (int j = 0; j < lineData.size(); j++) {
					String currentAttributeValue = (String) lineData.get(j);
					String attributeName = (String) attributeNamesList.get(j);
					String key = attributeName + currentAttributeValue;

					tempList.add(key);
					HashSet<String> set;
					if (distinctAttributeMap.containsKey(attributeName)) {
						set = (HashSet) distinctAttributeMap.get(attributeName);
					} else {
						set = new HashSet();
					}
					set.add(key);

					distinctAttributeMap.put(attributeName, set);
				}
				if (!dataMap.containsKey(tempList)) {
					dataMap.put(tempList, Integer.valueOf(1));
					for (String listKey : tempList) {
						HashSet<String> mapKey = new HashSet();
						mapKey.add(listKey);
						setMap(attributeValues, mapKey, lineNo);
					}
				} else {
					dataMap.put(tempList, Integer.valueOf(((Integer) dataMap.get(tempList)).intValue() + 1));
				}
			}
		}

		private static boolean checkEmptyValueInStringArray(ArrayList<String> lineData) {
			return lineData.contains("");
		}

		private static void setMap(Map<HashSet<String>, HashSet<String>> values, HashSet<String> key, int lineNo) {
			HashSet<String> tempSet = new HashSet();
			if (values.containsKey(key)) {
				tempSet.addAll((Collection) values.get(key));
			}
			tempSet.add("x" + lineNo);
			values.put(key, tempSet);
		}

		private void setDecisionAttributeValues() {
			HashSet<String> distinctDecisionValues = (HashSet) distinctAttributeMap.get(this.decisionAttribute);
			for (String value : distinctDecisionValues) {
				HashSet<String> newHash = new HashSet();
				HashSet<String> finalHash = new HashSet();
				newHash.add(value);
				if (decisionValuesMap.containsKey(value)) {
					finalHash.addAll((Collection) decisionValuesMap.get(value));
				}
				if (attributeValues.containsKey(newHash)) {
					finalHash.addAll((Collection) attributeValues.get(newHash));
				}
				decisionValuesMap.put(value, finalHash);
			}
		}

		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			fillAttributeValues();
			while (!this.actionMap.isEmpty()) {
				combineFrequentActions(context);
			}
			super.cleanup(context);
		}

		private void fillAttributeValues() {
			ArrayList<String> processedStableAttributes = new ArrayList();
			for (Map.Entry<HashSet<String>, HashSet<String>> set : attributeValues.entrySet()) {
				int support = ((HashSet) set.getValue()).size();
				if (support >= this.minSupport) {
					ArrayList<ArrayList<String>> outerSet = new ArrayList();
					ArrayList<String> innerSet = new ArrayList();
					ArrayList<String> attributeNames = new ArrayList();

					String primeAttribute = null;
					for (String element : set.getKey()) {
						innerSet.add(element);
						innerSet.add(element);
						outerSet.add(innerSet);

						primeAttribute = getAttributeName(element);
						attributeNames.add(primeAttribute);
						attributeNames.add(String.valueOf(support));
					}
					if (primeAttribute != null) {
						this.actionMap.put(outerSet, attributeNames);

						ArrayList<ArrayList<String>> forValue = new ArrayList();
						if (this.singleSet.get(primeAttribute) != null) {
							forValue = (ArrayList) this.singleSet.get(primeAttribute);
						}
						forValue.add(innerSet);

						this.singleSet.put(primeAttribute, forValue);
						if ((!isStable(primeAttribute)) && (!processedStableAttributes.contains(primeAttribute))) {
							ArrayList<String> distinctAttributeValuesAAR = new ArrayList();

							distinctAttributeValuesAAR.addAll((Collection) distinctAttributeMap.get(primeAttribute));
							for (int i = 0; i < distinctAttributeValuesAAR.size(); i++) {
								for (int j = 0; j < distinctAttributeValuesAAR.size(); j++) {
									if (i != j) {
										HashSet<String> left = new HashSet(Arrays
												.asList(new String[] { (String) distinctAttributeValuesAAR.get(i) }));

										HashSet<String> right = new HashSet(Arrays
												.asList(new String[] { (String) distinctAttributeValuesAAR.get(j) }));

										support = Math.min(((HashSet) attributeValues.get(left)).size(),
												((HashSet) attributeValues.get(right)).size());
										if (support >= this.minSupport) {
											processedStableAttributes.add(primeAttribute);

											outerSet = new ArrayList();
											innerSet = new ArrayList();

											innerSet.addAll(left);
											innerSet.addAll(right);

											outerSet.add(innerSet);
											attributeNames.set(1, String.valueOf(support));

											this.actionMap.put(outerSet, attributeNames);

											forValue = (ArrayList) this.singleSet.get(primeAttribute);

											forValue.add(innerSet);

											this.singleSet.put(primeAttribute, forValue);
										}
									}
								}
							}
						}
					}
				}
			}
		}

		public static String getAttributeName(String value1) {
			for (Map.Entry<String, HashSet<String>> entryValue : distinctAttributeMap.entrySet()) {
				if (((HashSet) entryValue.getValue()).contains(value1)) {
					return (String) entryValue.getKey();
				}
			}
			return null;
		}

		public boolean isStable(String value) {
			if (stableAttributeValuesList.containsAll((Collection) distinctAttributeMap.get(value))) {
				return true;
			}
			return false;
		}

		private void combineFrequentActions(Mapper<LongWritable, Text, Text, Text>.Context context) {
			ArrayList<ArrayList<String>> mainKey;
			ArrayList<String> mainValue;
			String attrKey;
			this.derivedActionMap = new HashMap();
			for (Map.Entry<ArrayList<ArrayList<String>>, ArrayList<String>> mainSet : this.actionMap.entrySet()) {
				mainKey = (ArrayList) mainSet.getKey();
				mainValue = (ArrayList) mainSet.getValue();
				for (Map.Entry<String, ArrayList<ArrayList<String>>> attrSet : this.singleSet.entrySet()) {
					attrKey = (String) attrSet.getKey();
					if (((mainValue.contains(this.decisionAttribute)) && (mainKey.contains(decisionFromAndToList)))
							|| ((attrKey.equals(this.decisionAttribute))
									&& (((ArrayList) attrSet.getValue()).contains(decisionFromAndToList))
									&& (!mainValue.contains(attrKey)))) {
						for (ArrayList<String> extraSet : (ArrayList<ArrayList<String>>) attrSet.getValue()) {
							ArrayList<String> toCheckIn = new ArrayList();
							ArrayList<String> toCheckOut = new ArrayList();
							ArrayList<String> newValue = new ArrayList();
							ArrayList<ArrayList<String>> newKey = new ArrayList();

							newKey.addAll(mainKey);
							newKey.add(extraSet);

							newValue.addAll(mainValue.subList(0, mainValue.size() - 1));

							newValue.add(attrKey);

							HashSet<String> newAssociation = new HashSet();
							for (ArrayList<String> checkMultipleValues : newKey) {
								String toAddIntoAssociations = "";

								String left = (String) checkMultipleValues.get(0);
								String right = (String) checkMultipleValues.get(1);

								toCheckIn.add(left);
								toAddIntoAssociations = toAddIntoAssociations + left + " -> ";

								toCheckOut.add(right);
								toAddIntoAssociations = toAddIntoAssociations + right;

								newAssociation.add(toAddIntoAssociations);
							}
							if (!this.associations.contains(newAssociation)) {
								int support = Math.min(findLERSSupport(toCheckIn), findLERSSupport(toCheckOut));

								newValue.add(String.valueOf(support));
								if (support >= this.minSupport) {
									this.derivedActionMap.put(newKey, newValue);
									printFrequentActions(newKey, newValue, context);

									this.associations.add(newAssociation);
								}
							}
						}
					}
				}
			}

			this.actionMap.clear();
			this.actionMap.putAll(this.derivedActionMap);
			this.derivedActionMap.clear();
		}

		private static int findLERSSupport(ArrayList<String> tempList) {
			int count = 0;
			for (Map.Entry<ArrayList<String>, Integer> entry : dataMap.entrySet()) {
				if (((ArrayList) entry.getKey()).containsAll(tempList)) {
					count += ((Integer) entry.getValue()).intValue();
				}
			}
			return count;
		}

		private void printFrequentActions(ArrayList<ArrayList<String>> key, ArrayList<String> value, Mapper<LongWritable, Text, Text, Text>.Context context) {
			if (value.contains(this.decisionAttribute)) {
				String rule = "";
				String decision = "";
				String decisionFrom = "";
				String decisionTo = "";
				int count = 0;
				ArrayList<String> actionFrom = new ArrayList();
				ArrayList<String> actionTo = new ArrayList();
				for (ArrayList<String> list : key) {
					if (((String) value.get(count)).equals(this.decisionAttribute)) {
						decisionFrom = (String) list.get(0);
						decisionTo = (String) list.get(1);
						decision = "(" + (String) value.get(count) + "," + decisionFrom + " ->  " + decisionTo + ")";
					} else {
						if (!rule.equals("")) {
							rule = rule + "^";
						}
						rule = rule + "(" + (String) value.get(count) + "," + (String) list.get(0) + " ->  "
								+ (String) list.get(1) + ")";

						actionFrom.add(list.get(0));
						actionTo.add(list.get(1));
					}
					count++;
				}
				if ((!rule.equals("")) && (!stableAttributeValuesList.containsAll(actionFrom))) {
					rule = rule + " ==> " + decision;

					String finalRule = rule;
					String finalDecisionFrom = decisionFrom;
					String finalDecisionTo = decisionTo;

					String suppConf = calculateAssociationActionRuleSupport(actionFrom, actionTo, finalDecisionFrom,
							finalDecisionTo, this.minSupport, this.minConfidence);
					if (!suppConf.equals("")) {
						try {
							Text key1 = new Text(finalRule);
							Text value1 = new Text(suppConf);
							if (key1.toString().indexOf(this.decisionAttribute) < key1.toString()
									.indexOf((String) decisionFromAndToList.get(0))) {
								if (key1.toString().indexOf((String) decisionFromAndToList.get(0)) < key1.toString()
										.indexOf((String) decisionFromAndToList.get(1))) {
									context.write(key1, value1);
								}
							}
						} catch (IOException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}

		public String calculateAssociationActionRuleSupport(ArrayList<String> actionFrom, ArrayList<String> actionTo, String decisionFrom, 
				String decisionTo, double minSupp, double minConf) {
			String supportConfidence = new String();

			ArrayList<String> left = new ArrayList();
			ArrayList<String> right = new ArrayList();

			left.addAll(actionFrom);
			left.add(decisionFrom);
			right.addAll(actionTo);
			right.add(decisionTo);

			double leftRuleSupport = findLERSSupport(left);
			double rightRuleSupport = findLERSSupport(right);
			double leftSupport = findLERSSupport(actionFrom);
			double rightSupport = findLERSSupport(actionTo);

			double support = Math.min(leftRuleSupport, rightRuleSupport);
			double confidence = leftRuleSupport / leftSupport * (rightRuleSupport / rightSupport) * 100.0D;
			if ((confidence >= minConf) && (support >= minSupp)) {
				supportConfidence = support + "," + confidence;
			}
			return supportConfidence;
		}
	}

	// Modified for Apriori Algorithm
	public static class JobReducer extends Reducer<Text, Text, Text, Text>
	{
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

			double minimumSupport = Double.parseDouble(context.getConfiguration().get("support"));

			double minimumConfidence = Double.parseDouble(context.getConfiguration().get("confidence"));

			double support = 0.0D;
			double confidence = 0.0D;
			
			DecimalFormat df = new DecimalFormat("###.##");

			for (Text val : values) {
				support = Double.valueOf(df.format(Double.parseDouble(val.toString().split(",")[0]))).doubleValue();

				confidence = Double.valueOf(df.format(Double.parseDouble(val.toString().split(",")[1]))).doubleValue();

				if (support >= minimumSupport && confidence >= minimumConfidence) { // Filter Action Rules according to user provided Support & Confidence
					context.write(key, new Text(val + " <----> " + "Good"));
				}

			}
		}
	}
}
