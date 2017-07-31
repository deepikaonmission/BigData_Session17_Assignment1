/*
<<<<<<<<<<<<<------------------ TASKS ------------------------->>>>>>>>>>>>>

TASK 1 ->> Write a program to read a text file and print the number of rows of data in the document "Sample.txt"
TASK 2 ->> Write a program to read a text file and print the number of words in the document "Sample.txt"

"Sample.txt" contents are as follows:

This is my first assignment.
It will count the number of lines in this document.
The total number of lines is 3.

TASK 3 ->> We have a document where the word separator is -, instead of space. Write a spark code, to obtain the count
 of the total number of words present in the document

"Sample Document.txt" contents are as follows:

This-is-my-first-assignment.
It-will-count-the-number-of-lines-in-this-document.
The-total-number-of-lines-is-3.
*/

import org.apache.spark.{SparkConf, SparkContext}      //As this is Spark program so import of SparkConf, SparkContext is required
                                                     //but import of spark, SparkConf, SparkContext will be available if libraries are specified in
                                                     //build.sbt file which will automatically import the library dependencies based on scala and spark version if specified

object Session17Assignment1 extends App{       //singleton class
  val conf = new SparkConf().setAppName("Session17Assign1").setMaster("local")
  //SparkConf allows to configure some of the common properties like master URL and application name
  //setMaster sets Master URL which is local in this case,
  //setAppName sets Application name here application name is "Sesison17Assign1" which can be helpful in case of debugging
  //finally these configuration settings are passed to "conf" object of SparkConf

  val sc = new SparkContext(conf)
  //SparkContext constructor is passed a SparkConf object (i.e. conf) which contains information about our application
  //now from here on, we can work on SparkContext using sc

  //<<<<<<<<<------------- TASK 1 Starts ----------->>>>>>>>>>

  //Reading text file by specifying the location inside sc.textFile(file:///directory/filename) this creates RDD i.e. firstRdd but data of RDD is not available until an action is performed on this RDD
  val firstRdd = sc.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 17/Assignments/Assignment1/Sample.txt")

  firstRdd.foreach(x => println(x))   //firstRdd -->>  RDD[String]  //since println can't be used with RDD, action i.e. foreach is used here to print the contents of firstRdd

  //Calculating count of number of rows of data in Sample.txt file and Printing the same
  val countRows = firstRdd.count()       //countRows -->> Long   //count() is an action that calculates the rows of data present in firstRdd and brings it in variable countRows

  println("Number of rows of data in text file 'Sample.txt' are : "+countRows)    //as countRows is of type Long, so println function can be used to print its value

  //********************* TASK 1 Ends ***********************


  //<<<<<<<<<------------ TASK 2 Starts ----------->>>>>>>>>>

  //Printing the number of words in the document (excluding ' ' space separator and full stop at the end of each row)

  val countWords = firstRdd.flatMap(x => x.split(" ")).count()      //countWords -->> Long
  // here flapMap transformation is applied on firstRdd, which flatmaps the contents of firstRdd based on " " space as separator
  // and thereby creates new RDD whose output is fed to count() action, which calculates the total words present inside it,
  // as count() is action, therefore, it returns value (i.e. total count of words) in countWords variable

  println("Number of words in 'Sample.txt' including separator 'space' are : "+countWords)   //as countWords is of type Long so println prints its value

  //********************* TASK 2 Ends ***********************


  //<<<<<<<<<------------ TASK 3 Starts ----------->>>>>>>>>>

  //Printing the number of words in the document "Sample Document.txt" (excluding '-' separator and full stop at the end of each row)

  //Reading text file by specifying the location inside sc.textFile(file:///directory/filename) this creates RDD i.e. firstRdd1 but data of RDD is not available until an action is performed on this RDD
  val firstRdd1 = sc.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 17/Assignments/Assignment1/Sample Document.txt")

  firstRdd1.foreach(x => println(x))   //firstRdd1 -->>  RDD[String]  //since println can't be used with RDD, action i.e. foreach is used here to print the contents of firstRdd1

  val countWordsExcSep = firstRdd1.flatMap(x => x.split("-")).count()      //countWordsExcSep  Long
  // here flapMap transformation is applied on firstRdd1, which flatmaps the contents of firstRdd based on '-' as separator
  // and thereby creates new RDD whose output is fed to count() action, which calculates the total words present inside it,
  // as count() is action, therefore, it returns value (i.e. total count of words) in countWordsExcSep variable

  println("Number of words in 'Sample Document.txt' excluding separator '-' are : "+countWordsExcSep)  ////as countWordsExcSep is of type Long so println prints its value

  //********************* TASK 3 Ends ***********************

}
