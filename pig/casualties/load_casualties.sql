LOAD DATA INFILE '/home/cloudera/workspace/casualties_example/casualties_rates.csv'    
INTO TABLE CASUALTIES 
FIELDS TERMINATED BY ';'   
OPTIONALLY ENCLOSED BY '"'   
LINES TERMINATED BY '\n'    
IGNORE 1 LINES;
