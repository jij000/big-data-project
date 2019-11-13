#run SparseMatrixMultiply
cd /home/cloudera/workspace/batch/
#delete output to run again
hadoop fs -rm -r /user/cloudera/wordcount/SparseMatrixMultiply_output
hadoop fs -mkdir /user/cloudera /user/cloudera/wordcount /user/cloudera/wordcount/SparseMatrixMultiply_input
hadoop fs -put input_SparseMatrixMultiply/* /user/cloudera/wordcount/SparseMatrixMultiply_input
#run
hadoop jar wordcount.jar edu.mum.cs.wordcount.SparseMatrixMultiply /user/cloudera/wordcount/SparseMatrixMultiply_input /user/cloudera/wordcount/SparseMatrixMultiply_output
read -n1 -r -p "Press any key to show the output..." key
hadoop fs -cat /user/cloudera/wordcount/SparseMatrixMultiply_output/*
#download
hadoop fs -get /user/cloudera/wordcount/SparseMatrixMultiply_output/* SparseMatrixMultiply_output
read -n1 -r -p "Press any key to continue..." key