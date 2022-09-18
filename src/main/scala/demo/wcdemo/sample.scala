package demo.wcdemo
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sample {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
    val sc = new SparkContext(conf)

if(args.length<2)
{
println("usage: Scala word count")
}
val rawData = sc.textFile(args(0))
val words = rawData.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val countfile = pairs.reduceByKey(_ + _)
val sortfile = countfile.sortByKey()
sortfile.saveAsTextFile(args(1))    
  }
}