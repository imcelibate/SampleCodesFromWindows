package org.iasoa.spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount_Cluster {
  
    def main(args : Array[String])= {
      
      println("Om Hari Sri Ganapathaye Namaha \nAvighnamasthu Sri Guruve Namaha")
      
     val sparkConf = new SparkConf().setMaster("local[1]").setAppName("My AMMA Spark")
     
     System.setProperty("hadoop.home.dir", "C://Amar//Tutorial//eclipse//workspace//bigdata//hadoop_home")
     
     val sparkContext = new SparkContext(sparkConf)
      
      val ipText = sparkContext.textFile("C://Amar//Tutorial//eclipse//workspace//sample_data//ip//Spark_WordCount.txt")
      
      val words = ipText.flatMap(line => (line.split(" ")))
      
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile("C://Amar//Tutorial//eclipse//workspace//sample_data//op//WordCount_Cluster_"+System.currentTimeMillis())
    }

}