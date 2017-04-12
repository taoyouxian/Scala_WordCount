import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Tao on 2017/4/12.
  */
object WordCount {

  def main(args: Array[String]) : Unit = {
    val  conf = new SparkConf().setAppName("wordCount")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/home/data/words")

    val lines = input.flatMap(line=>line.split(" "))
    val count = lines.map(word=>(word, 1)).reduceByKey{case (x,y)=>x+y}

    val output = count.saveAsTextFile("/home/data/wordsRes")
  }
}
