package assignment6
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {
  def main(args: Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("lab6")
    val sc = new SparkContext(conf)

    val inputPath = "input/final/";

    turnovers(sc, inputPath);
    playtime(sc, inputPath);
  }

  def turnovers(sc: SparkContext, inputPath: String) { 
    val playerRecords = sc.textFile(inputPath + "ppg.csv").map(_.split(",")).keyBy(_(3));
    val teamRecords = sc.textFile(inputPath + "standings.csv").map(_.split(",")).keyBy(_(0));

    val turnoversByTeam = playerRecords.combineByKey(
      v => v(24).toDouble,
      (acc: Double, v) => acc + v(24).toDouble,
      (acc1: Double, acc2: Double) => acc1 + acc2
    );
    val sorted = turnoversByTeam.sortBy(_._2);
    val topFive = sorted.take(6);
    topFive.foreach(vals => println(vals._1 + ", " + vals._2));
  }

  def playtime(sc: SparkContext, inputPath: String) { 
    val playerRecords = sc.textFile(inputPath + "ppg.csv").map(_.split(",")).map(vals => (vals(0), vals(5).toDouble, vals(28).toDouble));

    val playerData = playerRecords.sortBy(_._2, false).take(20);
    playerData.foreach(vals => println(vals._1 + ", " + vals._2 + ", " + vals._3));
  }

}
