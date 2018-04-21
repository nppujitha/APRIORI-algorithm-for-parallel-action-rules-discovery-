# APRIORI-algorithm-for-parallel-action-rules-discovery-
//** 
  ** README
  **
  **  PART 3
//


COMMANDS TO MOVE INPUT FOLDERS (W/ FILES)
-----------------------------------------
	hadoop fs -put Car
	hadoop fs -put Mammographic


COMMANDS TO MOVE JAR FILES
--------------------------
	hadoop fs -put Apriori.jar


COMMANDS TO RUN SOURCE CODE - CAR DATA
--------------------------------------
	hadoop jar Apriori.jar Apriori Car/car.attribute.txt Car/car.data.txt Car/ActionRulesOutput Car/AssociationRulesOutput


COMMANDS TO RUN SOURCE CODE - MAMMOGRAPHIC DATA
-----------------------------------------------
	hadoop jar Apriori.jar Apriori Mammographic/mammographic_attribute.txt Mammographic/mammographic_masses.data.txt Mammographic/ActionRulesOutput Mammographic/AssociationRulesOutput


COMMANDS TO GET OUTPUT
-----------------------
	hadoop fs -get Mammographic/ActionRulesOutput/part-r-00000  /home/cloudera/Mammographic/ActionRulesOutput.txt
	hadoop fs -get Mammographic/AssociationRulesOutput/part-r-00000  /home/cloudera/Mammographic/AssociationActionRulesOutput.txt
	
	--OR--
	
	hadoop fs -getmerge DATASET_FOLDER_NAME/ActionRulesOutput/* /users/UNCC_USERNAME/DATASET_FOLDER_NAME/ActionRulesOutput.txt
	hadoop fs -getmerge DATASET_FOLDER_NAME/AssociationRulesOutput/* /users/UNCC_USERNAME/DATASET_FOLDER_NAME/AssociationActionRulesOutput.txt
	

NOTE : DELETE OUTPUT FOLDERS BEFORE EACH START
----------------------------------------------
	hadoop fs -rm -r Mammographic

	--OR--

	hadoop fs -rm -r Car



