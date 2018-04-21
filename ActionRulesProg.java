package com.part3.group5;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ActionRulesProg {
	public static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
		int lineCount = 0;
		double minSupport;
		double minConfidence;
		String decisionAttribute;
		String decisionFrom;
		String decisionTo;
		public static ArrayList<String> allAttributesList;
		public static ArrayList<String> stableAttributesList;
		public static ArrayList<String> stableAttributeValues;
		public static ArrayList<ArrayList<String>> actionRules;
		static Map<ArrayList<String>, Integer> data;
		static Map<String, HashSet<String>> distinctAttributeValues;
		static Map<String, HashSet<String>> decisionValues;
		static Map<HashSet<String>, HashSet<String>> attributeValues;
		static Map<HashSet<String>, HashSet<String>> reducedAttributeValues;
		static Map<ArrayList<String>, HashSet<String>> markedValues;
		static Map<ArrayList<String>, HashSet<String>> possibleRules;
		static Map<ArrayList<String>, String> certainRules;

		public JobMapper() {
			// Initialize fields
			data = new HashMap<ArrayList<String>, Integer>();
			actionRules = new ArrayList();
			allAttributesList = new ArrayList();
			stableAttributesList = new ArrayList();
			stableAttributeValues = new ArrayList();
			attributeValues = new HashMap<HashSet<String>, HashSet<String>>();
			reducedAttributeValues = new HashMap<HashSet<String>, HashSet<String>>();
			distinctAttributeValues = new HashMap<String, HashSet<String>>();
			decisionValues = new HashMap<String, HashSet<String>>();
			certainRules = new HashMap<ArrayList<String>, String>();
			markedValues = new HashMap<ArrayList<String>, HashSet<String>>();
			possibleRules = new HashMap<ArrayList<String>, HashSet<String>>();
		}

		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// Set fields from info provided by user
			allAttributesList = new ArrayList(Arrays.asList(context.getConfiguration().getStrings("attributes")));

			stableAttributesList = new ArrayList(Arrays.asList(context.getConfiguration().getStrings("stable")));

			this.decisionAttribute = context.getConfiguration().getStrings("decision")[0];

			this.decisionFrom = context.getConfiguration().get("decisionFrom");
			this.decisionTo = context.getConfiguration().get("decisionTo");
			this.minSupport = Double.parseDouble(context.getConfiguration().get("support"));

			this.minConfidence = Double.parseDouble(context.getConfiguration().get("confidence"));

			super.setup(context);
		}

		protected void map(LongWritable key, Text inputValue, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			splitData(inputValue, this.lineCount);
			for (int i = 0; i < stableAttributesList.size(); i++) {
				HashSet<String> distinctStableValues = (HashSet<String>) distinctAttributeValues
						.get(stableAttributesList.get(i));
				for (String string : distinctStableValues) {
					if (!stableAttributeValues.contains(string)) {
						stableAttributeValues.add(string);
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

				ArrayList<String> tempList = new ArrayList<String>();
				for (int j = 0; j < lineData.size(); j++) {
					String currentAttributeValue = (String) lineData.get(j);
					String attributeName = (String) allAttributesList.get(j);
					String key = attributeName + currentAttributeValue;

					tempList.add(key);
					HashSet<String> set;
					if (distinctAttributeValues.containsKey(attributeName)) {
						set = (HashSet) distinctAttributeValues.get(attributeName);
					} else {
						set = new HashSet<String>();
					}
					set.add(key);
					distinctAttributeValues.put(attributeName, set);
				}
				if (!data.containsKey(tempList)) {
					data.put(tempList, Integer.valueOf(1));
					for (String listKey : tempList) {
						HashSet<String> mapKey = new HashSet<String>();
						mapKey.add(listKey);
						setMap(attributeValues, mapKey, lineNo);
					}
				} else {
					data.put(tempList, Integer.valueOf(((Integer) data.get(tempList)).intValue() + 1));
				}
			}
		}

		private static boolean checkEmptyValueInStringArray(ArrayList<String> lineData) {
			return lineData.contains("");
		}

		private static void setMap(Map<HashSet<String>, HashSet<String>> values, HashSet<String> key, int lineNo) {
			HashSet<String> tempSet = new HashSet<String>();
			if (values.containsKey(key)) {
				tempSet.addAll((Collection) values.get(key));
			}
			tempSet.add("x" + lineNo);
			values.put(key, tempSet);
		}

		private void setDecisionAttributeValues() {
			HashSet<String> distinctDecisionValues = (HashSet) distinctAttributeValues.get(this.decisionAttribute);
			for (String value : distinctDecisionValues) {
				HashSet<String> newHash = new HashSet<String>();
				HashSet<String> finalHash = new HashSet<String>();
				newHash.add(value);
				if (decisionValues.containsKey(value)) {
					finalHash.addAll((Collection) decisionValues.get(value));
				}
				if (attributeValues.containsKey(newHash)) {
					finalHash.addAll((Collection) attributeValues.get(newHash));
				}
				decisionValues.put(value, finalHash);
				attributeValues.remove(newHash);
			}
		}

		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			performLERS();

			generateActionRules(context);

			super.cleanup(context);
		}

		private void performLERS() {
			int loopCount = 0;
			while (!attributeValues.isEmpty()) {
				for (Map.Entry<HashSet<String>, HashSet<String>> set : attributeValues.entrySet()) {
					ArrayList<String> setKey = new ArrayList<String>();
					setKey.addAll((Collection) set.getKey());

					HashSet<String> setValue = (HashSet) set.getValue();
					if (!((HashSet) set.getValue()).isEmpty()) {
						for (Map.Entry<String, HashSet<String>> decisionSet : decisionValues.entrySet()) {
							if (((HashSet) decisionSet.getValue()).containsAll((Collection) set.getValue())) {
								certainRules.put(setKey, decisionSet.getKey());
								markedValues.put(setKey, set.getValue());
								break;
							}
						}
						if (!markedValues.containsKey(setKey)) {
							HashSet<String> possibleRulesSet = new HashSet<String>();
							for (Map.Entry<String, HashSet<String>> decisionSet : decisionValues.entrySet()) {
								possibleRulesSet.add(decisionSet.getKey());
							}
							if (possibleRulesSet.size() > 0) {
								possibleRules.put(setKey, possibleRulesSet);
							}
						}
					}
				}
				
				removeMarkedValues();

				combinePossibleRules();
			}
		}

		static int calculateSupportLERS(ArrayList<String> key, String value) {
			// Method to calculate Support values
			ArrayList<String> tempList = new ArrayList<String>();
			for (String val : key) {
				if (!val.equals("")) {
					tempList.add(val);
				}
			}
			if (!value.equals("")) {
				tempList.add(value);
			}
			return findLERSSupport(tempList);
		}

		private static int findLERSSupport(ArrayList<String> tempList) {
			int count = 0;
			for (Map.Entry<ArrayList<String>, Integer> entry : data.entrySet()) {
				if (((ArrayList) entry.getKey()).containsAll(tempList)) {
					count += ((Integer) entry.getValue()).intValue();
				}
			}
			return count;
		}

		static String calculateConfidenceLERS(ArrayList<String> key, String value) {
			// Method to calculate Support values
			int num = calculateSupportLERS(key, value);
			int den = calculateSupportLERS(key, "");
			int confidence = 0;
			if (den != 0) {
				confidence = num * 100 / den;
			}
			return String.valueOf(confidence);
		}

		private static void removeMarkedValues() {
			for (Map.Entry<ArrayList<String>, HashSet<String>> markedSet : markedValues.entrySet()) {
				attributeValues.remove(new HashSet((Collection) markedSet.getKey()));
			}
		}

		private static void combinePossibleRules() {
			Set<ArrayList<String>> keySet = possibleRules.keySet();
			ArrayList<ArrayList<String>> keyList = new ArrayList();
			keyList.addAll(keySet);
			for (int i = 0; i < possibleRules.size(); i++) {
				for (int j = i + 1; j < possibleRules.size(); j++) {
					HashSet<String> combinedKeys = new HashSet((Collection) keyList.get(i));

					combinedKeys.addAll(new HashSet((Collection) keyList.get(j)));
					if (!checkSameGroup(combinedKeys)) {
						combineAttributeValues(combinedKeys);
					}
				}
			}
			removeRedundantValues();
			clearAttributeValues();
			possibleRules.clear();
		}

		public static boolean checkSameGroup(HashSet<String> combinedKeys) {
			ArrayList<String> combinedKeyAttributes = new ArrayList<String>();
			Map.Entry<String, HashSet<String>> singleAttribute;
			for (Iterator i$ = distinctAttributeValues.entrySet().iterator(); i$.hasNext();) {
				singleAttribute = (Map.Entry) i$.next();
				for (String key : combinedKeys) {
					if (((HashSet) singleAttribute.getValue()).contains(key)) {
						if (!combinedKeyAttributes.contains(singleAttribute.getKey())) {
							combinedKeyAttributes.add(singleAttribute.getKey());
						} else {
							return true;
						}
					}
				}
			}

			return false;
		}

		private static void combineAttributeValues(HashSet<String> combinedKeys) {
			HashSet<String> combinedValues = new HashSet<String>();
			for (Map.Entry<HashSet<String>, HashSet<String>> attributeValue : attributeValues.entrySet()) {
				if (combinedKeys.containsAll((Collection) attributeValue.getKey())) {
					if (combinedValues.isEmpty()) {
						combinedValues.addAll((Collection) attributeValue.getValue());
					} else {
						combinedValues.retainAll((Collection) attributeValue.getValue());
					}
				}
			}
			if (combinedValues.size() != 0) {
				reducedAttributeValues.put(combinedKeys, combinedValues);
			}
		}

		private static void removeRedundantValues() {
			Map.Entry<HashSet<String>, HashSet<String>> reducedAttributeValue;
			HashSet<String> mark = new HashSet<String>();
			for (Iterator i$ = reducedAttributeValues.entrySet().iterator(); i$.hasNext();) {
				reducedAttributeValue = (Map.Entry) i$.next();
				for (Map.Entry<HashSet<String>, HashSet<String>> attributeValue : attributeValues.entrySet()) {
					if ((((HashSet) attributeValue.getValue())
							.containsAll((Collection) reducedAttributeValue.getValue()))
							|| (((HashSet) reducedAttributeValue.getValue()).isEmpty())) {
						mark.addAll((Collection) reducedAttributeValue.getKey());
					}
				}
			}

			reducedAttributeValues.remove(mark);
		}

		private static void clearAttributeValues() {
			attributeValues.clear();
			for (Map.Entry<HashSet<String>, HashSet<String>> reducedAttributeValue : reducedAttributeValues
					.entrySet()) {
				attributeValues.put(reducedAttributeValue.getKey(), reducedAttributeValue.getValue());
			}
			reducedAttributeValues.clear();
		}

		// Generate ActionRules from data
		public ArrayList<String> generateActionRules(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			ArrayList<String> actions = null;
			Map.Entry<ArrayList<String>, String> certainRules1;

			String rule = "";
			ArrayList<String> rules = new ArrayList<String>();
			for (Iterator i$ = certainRules.entrySet().iterator(); i$.hasNext();) {
				certainRules1 = (Map.Entry) i$.next();

				String certainRules1Value = (String) certainRules1.getValue();
				if (certainRules1Value.equals(this.decisionFrom)) {
					for (Map.Entry<ArrayList<String>, String> certainRules2 : certainRules.entrySet()) {
						ArrayList<String> certainRules1Key = (ArrayList<String>) certainRules1.getKey();

						ArrayList<String> certainRules2Key = (ArrayList<String>) certainRules2.getKey();
						if ((!certainRules1Key.equals(certainRules2Key))
								&& (((String) certainRules2.getValue()).equals(this.decisionTo))
								&& (checkStableAttributes(certainRules1Key, certainRules2Key))) {
							String primeAttribute = "";
							if (checkRulesSubSet(certainRules1Key, certainRules2Key)) {
								ArrayList<String> checkCertainValues1 = (ArrayList) certainRules1.getKey();

								ArrayList<String> tempList = new ArrayList<String>();

								rule = "";
								ArrayList<String> actionFrom = new ArrayList<String>();
								ArrayList<String> actionTo = new ArrayList<String>();
								actions = new ArrayList<String>();
								String value1;
								for (Iterator i2$ = checkCertainValues1.iterator(); i2$.hasNext();) {
									value1 = (String) i2$.next();
									if (stableAttributeValues.contains(value1)) {
										if (!actionTo.contains(value1)) {
											rule = formRule(rule, value1, value1);

											actionFrom.add(value1);
											actionTo.add(value1);
											actions.add(getAction(value1, value1));
										}
									} else {
										primeAttribute = getAttributeName(value1);

										ArrayList<String> checkCertainValues2 = (ArrayList<String>) certainRules2
												.getKey();
										for (String value2 : checkCertainValues2) {
											if (stableAttributeValues.contains(value2)) {
												if (!actionTo.contains(value2)) {
													rule = formRule(rule, value2, value2);

													actionFrom.add(value2);
													actionTo.add(value2);
													actions.add(getAction(value2, value2));
												}
											} else if (!getAttributeName(value2).equals(primeAttribute)) {
												tempList.add(value2);
											} else if ((getAttributeName(value2).equals(primeAttribute))
													&& (!actionTo.contains(value2))) {
												rule = formRule(rule, value1, value2);

												actionFrom.add(value1);
												actionTo.add(value2);
												actions.add(getAction(value1, value2));
											}
										}
									}
								}

								for (String missedValues : tempList) {
									if (!actionTo.contains(missedValues)) {
										rule = formRule(rule, "", missedValues);

										actionFrom.add("");
										actionTo.add(missedValues);
										actions.add(getAction("", missedValues));
									}
								}
								printActionRule(actionFrom, actionTo, actions, rule, context);

								printExtraRules(actionFrom, actionTo, context);
							}
						}
					}
				}
			}

			return rules;
		}

		private static boolean checkStableAttributes(ArrayList<String> key, ArrayList<String> key2) throws IOException, InterruptedException {
			List<String> stableAttributes1 = new ArrayList<String>();
			List<String> stableAttributes2 = new ArrayList<String>();
			for (String value : key) {
				if (stableAttributeValues.contains(value)) {
					stableAttributes1.add(value);
				}
			}
			for (String value : key2) {
				if (stableAttributeValues.contains(value)) {
					stableAttributes2.add(value);
				}
			}
			if (stableAttributes2.containsAll(stableAttributes1)) {
				return true;
			}
			return false;
		}

		private boolean checkRulesSubSet(ArrayList<String> certainRules1, ArrayList<String> certainRules2) {
			ArrayList<String> primeAttributes1 = new ArrayList<String>();
			ArrayList<String> primeAttributes2 = new ArrayList<String>();
			for (String string : certainRules1) {
				String attributeName = getAttributeName(string);
				if (!isStable(attributeName)) {
					primeAttributes1.add(attributeName);
				}
			}
			for (String string : certainRules2) {
				String attributeName = getAttributeName(string);
				if (!isStable(attributeName)) {
					primeAttributes2.add(attributeName);
				}
			}
			if (primeAttributes2.containsAll(primeAttributes1)) {
				return true;
			}
			return false;
		}

		public static String getAttributeName(String value1) {
			for (Map.Entry<String, HashSet<String>> entryValue : distinctAttributeValues.entrySet()) {
				if (((HashSet) entryValue.getValue()).contains(value1)) {
					return (String) entryValue.getKey();
				}
			}
			return null;
		}

		private static String formRule(String rule, String value1, String value2) {
			if (!rule.isEmpty()) {
				rule = rule + "^";
			}
			rule = rule + "(" + getAttributeName(value2) + "," + getAction(value1, value2) + ")";

			return rule;
		}

		private static String getAction(String left, String right) {
			return left + " -> " + right;
		}

		private void printActionRule(ArrayList<String> actionFrom, ArrayList<String> actionTo,
				ArrayList<String> actions, String rule, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int support = calculateSupportLERS(actionTo, this.decisionTo);
			if (support != 0) {
				String oldConf = String.valueOf(Integer.parseInt(calculateConfidenceLERS(actionFrom, this.decisionFrom)) * 
						Integer.parseInt(calculateConfidenceLERS(actionTo, this.decisionTo)) / 100);

				String newConf = calculateConfidenceLERS(actionTo, this.decisionTo);
				if ((support >= this.minSupport) && (Double.parseDouble(oldConf) >= this.minConfidence)
						&& (!oldConf.equals("0")) && (!newConf.equals("0"))) {
					if (actions != null) {
						Collections.sort(actions);
						if (!actionRules.contains(actions)) {
							actionRules.add(actions);
							try {
								Text key1 = new Text(rule + " ==> " + "(" + this.decisionAttribute + ","
										+ this.decisionFrom + " -> " + this.decisionTo + ")");

								Text value1 = new Text(support + ":" + newConf);

								context.write(key1, value1);
							} catch (IOException e) {
								e.printStackTrace();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}

		private void printExtraRules(ArrayList<String> actionFrom, ArrayList<String> actionTo, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			ArrayList<String> stableValues = getStableValues(actionTo);
			ArrayList<String> attributeValues = getAttributeValues(stableValues, this.decisionFrom, actionFrom);

			ArrayList<String> toBeAddedAttributes = getNewAttributes(actionFrom, actionTo, stableValues);
			for (String attributeValue : toBeAddedAttributes) {
				ArrayList<String> tempAttributeValues = new ArrayList<String>();
				tempAttributeValues.add(attributeValue);

				ArrayList<String> checkList = getAttributeValues(stableValues, "", tempAttributeValues);
				if ((attributeValues.containsAll(checkList)) && (!checkList.isEmpty())) {
					String subRule = new String();
					ArrayList<String> subActionFrom = new ArrayList<String>();
					ArrayList<String> subActionTo = new ArrayList<String>();
					ArrayList<String> subActions = new ArrayList<String>();
					if (isStable(getAttributeName(attributeValue))) {
						subActionFrom.addAll(actionFrom);
						subActionFrom.add(attributeValue);

						subActionTo.addAll(actionTo);
						subActionTo.add(attributeValue);
					} else {
						subActionFrom = getSubActionFrom(actionFrom, actionTo, attributeValue);

						subActionTo.addAll(actionTo);
					}
					subRule = getSubRule(subActionFrom, subActionTo, subActions);
					try {
						printActionRule(subActionFrom, subActionTo, subActions, subRule, context);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}

		private ArrayList<String> getNewAttributes(ArrayList<String> actionFrom, ArrayList<String> actionTo, ArrayList<String> stableValues) {
			ArrayList<String> stableAttributes = new ArrayList<String>();
			ArrayList<String> flexibleAttributes = new ArrayList<String>();
			ArrayList<String> newAttributes = new ArrayList<String>();
			for (String string : stableValues) {
				stableAttributes.add(getAttributeName(string));
			}
			for (String string : actionTo) {
				flexibleAttributes.add(getAttributeName(string));
			}
			for (Map.Entry<String, HashSet<String>> mapValue : distinctAttributeValues.entrySet()) {
				String mapKey = (String) mapValue.getKey();
				HashSet<String> mapValues = (HashSet) mapValue.getValue();
				if ((!mapKey.equals(this.decisionAttribute))
						&& ((stableAttributes.size() == 0) || (!stableAttributes.contains(mapKey)))) {
					if (isStable(mapKey)) {
						newAttributes.addAll(mapValues);
					} else if ((!flexibleAttributes.isEmpty()) && (flexibleAttributes.contains(mapKey))) {
						for (String setValue : mapValues) {
							if ((!actionFrom.contains(setValue)) && (!actionTo.contains(setValue))) {
								newAttributes.add(setValue);
							}
						}
					}
				}
			}
			return newAttributes;
		}

		private ArrayList<String> getStableValues(ArrayList<String> actionFrom) {
			ArrayList<String> stableValues = stableAttributeValues;
			ArrayList<String> toBeAdded = new ArrayList<String>();
			for (String value : actionFrom) {
				if (stableValues.contains(value)) {
					toBeAdded.add(value);
				}
			}
			return toBeAdded;
		}

		private ArrayList<String> getAttributeValues(ArrayList<String> stableValues, String decisionFrom, ArrayList<String> actionFrom) {
			ArrayList<String> temp = new ArrayList<String>();
			ArrayList<String> attributeValues = new ArrayList<String>();
			int lineCount = 0;

			temp.addAll(stableValues);
			for (String from : actionFrom) {
				if (!from.equals("")) {
					temp.add(from);
				}
			}
			if (!decisionFrom.equals("")) {
				temp.add(decisionFrom);
			}
			for (Map.Entry<ArrayList<String>, Integer> data1 : data.entrySet()) {
				lineCount++;
				if (((ArrayList) data1.getKey()).containsAll(temp)) {
					attributeValues.add("x" + lineCount);
				}
			}
			return attributeValues;
		}

		private ArrayList<String> getSubActionFrom(ArrayList<String> actionFrom, ArrayList<String> actionTo,
				String alternateActionFrom) {
			ArrayList<String> finalActionFrom = new ArrayList<String>();
			for (int i = 0; i < actionTo.size(); i++) {
				HashSet<String> checkSameSet = new HashSet<String>();

				checkSameSet.add(alternateActionFrom);
				checkSameSet.add(actionTo.get(i));
				if (checkSameGroup(checkSameSet)) {
					finalActionFrom.add(alternateActionFrom);
				} else if (i < actionFrom.size()) {
					finalActionFrom.add(actionFrom.get(i));
				}
			}
			return finalActionFrom;
		}

		private String getSubRule(ArrayList<String> subActionFrom, ArrayList<String> subActionTo,
				ArrayList<String> subActions) {
			String rule = "";
			for (int i = 0; i < subActionFrom.size(); i++) {
				rule = formRule(rule, (String) subActionFrom.get(i), (String) subActionTo.get(i));
				subActions.add(getAction((String) subActionFrom.get(i), (String) subActionTo.get(i)));
			}
			return rule;
		}

		public boolean isStable(String value) {
			if (stableAttributeValues.containsAll((Collection) distinctAttributeValues.get(value))) {
				return true;
			}
			return false;
		}
	}

	public static class JobReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			double support = 0.0D;
			double confidence = 0.0D;
			DecimalFormat df = new DecimalFormat("###.##");
			int mapper = Integer.parseInt(context.getConfiguration().get("mapred.map.tasks"));

			int min_presence = mapper / 2;
			int sum = 0;
			for (Text val : values) {
				sum++;
				support += Double.valueOf(df.format(Double.parseDouble(val.toString().split(":")[0]))).doubleValue();

				confidence += Double.valueOf(df.format(Double.parseDouble(val.toString().split(":")[1]))).doubleValue();
			}
			if (sum >= min_presence) {
				context.write(key, new Text(sum + "" + " [Support: " + Double.parseDouble(df.format(support / sum))
						+ ", Confidence: " + Double.parseDouble(df.format(confidence / sum)) + "%]"));
			}
		}
	}
}
