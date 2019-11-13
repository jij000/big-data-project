# big-data-project

## The Relationship of Question -> Program

| Question | Program |
| --------- | ----------- |
| Part 1 (a)-(c) | WordCount.java |
| Part 1 (d) | InMapperWordCount.java |
| Part 1 (e) | AverageComputation.java |
| Part 1 (f) | InMapperAverageComputation.java |
| Part 2 | RelativeFrequenciesPair.java |
| Part 3 | RelativeFrequenciesStripe.java |
| Part 4 | RelativeFrequenciesMapPairRedueStripe.java |
| Part 5 | MatrixMultiply.java SparseMatrixMultiply.java |

## About How to Show input, output and batch file to execute your program at command line in Hadoop

1. please copy the batch folder to vm this path
    /home/cloudera/workspace/batch/
2. open terminal and change the current path
```sh
cd /home/cloudera/workspace/batch/
``` 
3. run the command in cloudera vm, and it will copy output file to local output folder
```sh
sh RelativeFrequenciesPair.sh
```
4. a sample of batch file
``` sh
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
```

