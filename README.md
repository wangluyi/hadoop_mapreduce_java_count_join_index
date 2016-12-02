# hadoop_mapreduce_java_count_join_index

data set that contains:
1. plot_summaries.txt: movieID and movie plot in each line
2. movie.metadata.tsv: metadata about each movie, like name, language, etc.
3. character.metadata.tsv: metadata about characters in each movie, like actor, gender, etc.
4. README.txt: contains a description of the files above files above.
4. stop.txt: common stop words in English: a, the, and, etc.

There are 4 tasks. You have to solve them using MapReduce:
1: Count the total number of words in plot_summaries.txt.
2.1: Get the top 10 most frequently used words from plot_summaries.txt in descending order.
2.2: Get the top 10 most frequently used words (excluding stop words) from plot_summaries.txt in descending order.
3: Join movie.metadata.tsv and character.metadata.tsv to find actors/actresses in each movie.
4: Build an inverted index of the words for the movies in plot_summaries.txt.

java files have the following names: WordCount.java (for task 1), WordFreq.java (for task 2.1), WordFreqStop.java (for task 2.2), Join.java (for task 3), Index.java (for task 4).
arguments: (1)path to the Java file, (2)output file. 
