#run RelativeFrequenciesMapPairRedueStripe
cd /home/cloudera/workspace/batch/
#delete output to run again
hadoop fs -rm -r /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_output
hadoop fs -mkdir /user/cloudera /user/cloudera/wordcount /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_input
hadoop fs -put input/file* /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_input
#run
hadoop jar wordcount.jar edu.mum.cs.wordcount.RelativeFrequenciesMapPairRedueStripe /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_input /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_output
read -n1 -r -p "Press any key to show the output..." key
hadoop fs -cat /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_output/*
#download
hadoop fs -get /user/cloudera/wordcount/RelativeFrequenciesMapPairRedueStripe_output/* RelativeFrequenciesMapPairRedueStripe_output
read -n1 -r -p "Press any key to continue..." key