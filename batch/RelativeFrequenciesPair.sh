#run RelativeFrequenciesPair
cd /home/cloudera/workspace/batch/
#delete input and output to run again
hadoop fs -rm -r /user/cloudera/wordcount/RelativeFrequenciesPair_input
hadoop fs -rm -r /user/cloudera/wordcount/RelativeFrequenciesPair_output
hadoop fs -mkdir /user/cloudera /user/cloudera/wordcount /user/cloudera/wordcount/RelativeFrequenciesPair_input
hadoop fs -put input/file* /user/cloudera/wordcount/RelativeFrequenciesPair_input
#run
hadoop jar wordcount.jar edu.mum.cs.wordcount.RelativeFrequenciesPair /user/cloudera/wordcount/RelativeFrequenciesPair_input /user/cloudera/wordcount/RelativeFrequenciesPair_output
read -n1 -r -p "Press any key to show the output..." key
hadoop fs -cat /user/cloudera/wordcount/RelativeFrequenciesPair_output/*
#download
hadoop fs -get /user/cloudera/wordcount/RelativeFrequenciesPair_output/* RelativeFrequenciesPair_output
read -n1 -r -p "Press any key to continue..." key