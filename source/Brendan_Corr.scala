package example

import breeze.linalg.reverse

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.dmg.pmml.False

import scala.collection._

object App {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "c:/winutils")
    val conf = new SparkConf().setAppName("Assignment 5").setMaster("local[4]")
    val sc = new SparkContext(conf)


    //standings
    sc.textFile("standings.txt").map(x => (x.split(",")(0), x.split(",")(1).split("-")(0).toInt)).sortBy(_._2, false).collect().foreach(x => println(x._1))


//    What 10 coaches promote the most scoring per game? Print coach and the points the team scores per game along with team and record. (Brendan)

    val coaches = sc.textFile("coaches.txt").map(x => (x.split(",")(1), x.split(",")(0)))

    val team_scores = sc.textFile("players.txt").map(x => (x.split(",")(3), x.split(",")(28).toDouble)).reduceByKey({(x, y) => x+y})

    println(" ")

    coaches.join(team_scores).map(x=> (x._2._1, x._1, x._2._2.round)).sortBy(_._3, false).collect().take(10).foreach(x => println(x._1 + ", " + x._2 + ", " + x._3))


    //    What players might make the 50, 40, 90 club (fg%, 3p%, ft%)? (within 5% for each category) Return all the eligible players and order by the highest shooting percentage for each (Brendan)

    val players = sc.textFile("players.txt").map(x => (x.split(",")(0).split("\\\\")(0), (x.split(",")(9), x.split(",")(12), x.split(",")(19))))

    val temp = players.filter(x => x._2._1.toDouble > .47 ).filter(x => x._2._3.toDouble>.87)
    println(" ")
    temp.filter(x=> x._2._2.toDouble > .37).map(x=> (x._1,(x._2._1, x._2._2, x._2._3, x._2._1.toDouble + x._2._2.toDouble +x._2._2.toDouble))).sortBy(_._2._2, false).collect().foreach(x => println(x._1 + ", fg: " + x._2._1 + " 3pt: " + x._2._2 + " ft: " + x._2._3))



  }



}

