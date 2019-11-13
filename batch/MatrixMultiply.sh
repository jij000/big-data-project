#run MatrixMultiply
cd /home/cloudera/workspace/batch/
#delete output to run again
hadoop fs -rm -r /user/cloudera/wordcount/MatrixMultiply_output
hadoop fs -mkdir /user/cloudera /user/cloudera/wordcount /user/cloudera/wordcount/MatrixMultiply_input
hadoop fs -put input_MatrixMultiply/* /user/cloudera/wordcount/MatrixMultiply_input
#run
hadoop jar wordcount.jar edu.mum.cs.wordcount.MatrixMultiply /user/cloudera/wordcount/MatrixMultiply_input /user/cloudera/wordcount/MatrixMultiply_output
read -n1 -r -p "Press any key to show the output..." key
hadoop fs -cat /user/cloudera/wordcount/MatrixMultiply_output/*
#download
hadoop fs -get /user/cloudera/wordcount/MatrixMultiply_output/* MatrixMultiply_output
read -n1 -r -p "Press any key to continue..." key