#run RelativeFrequenciesStripe
cd /home/cloudera/workspace/batch/
#delete output to run again
hadoop fs -rm -r /user/cloudera/wordcount/RelativeFrequenciesStripe_output
hadoop fs -mkdir /user/cloudera /user/cloudera/wordcount /user/cloudera/wordcount/RelativeFrequenciesStripe_input
hadoop fs -put input/file* /user/cloudera/wordcount/RelativeFrequenciesStripe_input
#run
hadoop jar wordcount.jar edu.mum.cs.wordcount.RelativeFrequenciesStripe /user/cloudera/wordcount/RelativeFrequenciesStripe_input /user/cloudera/wordcount/RelativeFrequenciesStripe_output
read -n1 -r -p "Press any key to show the output..." key
hadoop fs -cat /user/cloudera/wordcount/RelativeFrequenciesStripe_output/*
#download
hadoop fs -get /user/cloudera/wordcount/RelativeFrequenciesStripe_output/* RelativeFrequenciesStripe_output
read -n1 -r -p "Press any key to continue..." key