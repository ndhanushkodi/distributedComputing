import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by ndhanushkodi on 10/3/15.
 */
class SparkWordCountJob {
  def main(args:Array[String]):Unit={

    val conf = new SparkConf().setMaster("local").setAppName("SparkWordCount")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/home/ndhanushkodi/test.txt")
    //spark context is bridge from coder to spark engine

    val counts = input.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    counts.saveAsTextFile("counts.txt")
  }

}
