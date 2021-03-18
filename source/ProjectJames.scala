import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object ProjectJames {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    most_three_pointers(sc)
    players_10_ppg(sc)
  }
  def most_three_pointers(sc: SparkContext) {

    val team_map = Map("ATL" -> "Atlanta Hawks",
                    "BOS" -> "Boston Celtics",
                    "BRK" -> "Brooklyn Nets",
                    "CHO" -> "Charlotte Hornets",
                    "CHI" -> "Chicago Bulls",
                    "CLE" -> "Cleveland Cavaliers",
                    "DAL" -> "Dallas Mavericks",
                    "DEN" -> "Denver Nuggets",
                    "DET" -> "Detroit Pistons",
                    "GSW" -> "Golden State Warriors",
                    "HOU" -> "Houston Rockets",
                    "IND" -> "Indiana Pacers",
                    "LAC" -> "Los Angeles Clippers",
                    "LAL" -> "Los Angeles Lakers",
                    "MEM" -> "Memphis Grizzlies",
                    "MIA" -> "Miami Heat",
                    "MIL" -> "Milwaukee Bucks",
                    "MIN" -> "Minnesota Timberwolves",
                    "NOP" -> "New Orleans Pelicans",
                    "NYK" -> "New York Knicks",
                    "OKC" -> "Oklahoma City Thunder",
                    "ORL" -> "Orlando Magic",
                    "PHI" -> "Philadelphia 76ers",
                    "PHO" -> "Phoenix Suns",
                    "POR" -> "Portland Trailblazers",
                    "SAC" -> "Sacramento Kings",
                    "SAS" -> "San Antonio Spurs",
                    "TOR" -> "Toronto Raptors",
                    "UTA" -> "Utah Jazz",
                    "WAS" -> "Washington Wizards"
    )

    val ppg = sc.textFile("ppg.csv").map(x=> (x.split(",")(3), (x.split(",")(11).toDouble * x.split(",")(4).toDouble).toInt))
    val tot_filtered = ppg.filter(x=> x._1 != "TOT").map(x=> (team_map(x._1), x._2))
    val threes_by_team = tot_filtered.reduceByKey((x, y) => x + y)
    val most_threes = threes_by_team.map(x => x._2).top(1)
    threes_by_team.filter(x => x._2 == most_threes(0)).collect().foreach(println)
  }

  def players_10_ppg(sc: SparkContext) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val team_map = Map("ATL" -> "Atlanta Hawks",
      "BOS" -> "Boston Celtics",
      "BRK" -> "Brooklyn Nets",
      "CHO" -> "Charlotte Hornets",
      "CHI" -> "Chicago Bulls",
      "CLE" -> "Cleveland Cavaliers",
      "DAL" -> "Dallas Mavericks",
      "DEN" -> "Denver Nuggets",
      "DET" -> "Detroit Pistons",
      "GSW" -> "Golden State Warriors",
      "HOU" -> "Houston Rockets",
      "IND" -> "Indiana Pacers",
      "LAC" -> "Los Angeles Clippers",
      "LAL" -> "Los Angeles Lakers",
      "MEM" -> "Memphis Grizzlies",
      "MIA" -> "Miami Heat",
      "MIL" -> "Milwaukee Bucks",
      "MIN" -> "Minnesota Timberwolves",
      "NOP" -> "New Orleans Pelicans",
      "NYK" -> "New York Knicks",
      "OKC" -> "Oklahoma City Thunder",
      "ORL" -> "Orlando Magic",
      "PHI" -> "Philadelphia 76ers",
      "PHO" -> "Phoenix Suns",
      "POR" -> "Portland Trailblazers",
      "SAC" -> "Sacramento Kings",
      "SAS" -> "San Antonio Spurs",
      "TOR" -> "Toronto Raptors",
      "UTA" -> "Utah Jazz",
      "WAS" -> "Washington Wizards"
    )

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val ppg = sc.textFile("ppg.csv").map(x=> (x.split(",")(3), (x.split(",")(0).split('\\')(0), x.split(",")(28))))
    val ppg_20 = ppg.filter(x=> x._2._2.toDouble >= 10 && x._1 != "TOT").map(x=> (team_map(x._1), x._2))
    sc.parallelize(ppg_20.countByKey().toSeq).mapValues(v=> v).top(5).sortWith(_._2 > _._2).foreach(println)

  }
}

