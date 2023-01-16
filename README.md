# IPL_Data_Analysis_Using_Hadoop
IPL Data analysis using Hadoop MapReduce

In this project I have created two tasks in the given IPL dataset

1. The number of deliveries in which a wicket was taken, extra runs were given and no runs
were scored. (DeliveriesData class)
2. The Total number of wickets taken by each team (WicketCount)

First you need to pull the  docker image using below command:

docker pull suhothayan/hadoop-hive-pig:2.7.1

Then, you need to create the Java classes with map reduce codes. (I have mentioned the classes i created)

Then you need to run the docker container using below command.
Note: port mapping is upto you & replace the necessary details accordingly.

docker run -p 8088:8088 -p 50070:50070 --name <Container_Name> -v <pathforthedataset>:/resources -d suhothayan/hadoop-hive-pig:2.7.1 


--------------- Execute the docker continer------------------

docker exec -it <Container_Name> bash 

redirec to resource folder : 

cd resource/

Write the dataset into hdfs : 

hdfs dfs -put <inputfolderName> <hdfsfolderName>

#####################################################Task 01 #################################################

yarn jar <projectName>/target/<jarfileName>.jar <classname> <inputfolderName>/Ipl.csv output/DeliveryData

read the output file> 
hdfs dfs -cat output/DeliveryData/part-r-00000



###################################################Task 02 ###################################################################

yarn jar <projectName>/target/<jarfileName>.jar <classname> <inputfolderName>/Ipl.csv output/WicketCount

hdfs dfs -cat output/WicketCount/part-r-00000


