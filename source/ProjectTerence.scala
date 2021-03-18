// Terence Tong

package project

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ProjectTerence {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("labs").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val team_map = Map[String, String](
      "ATL" -> "Atlanta Hawks",
      "BOS" -> "Boston Celtics",
      "BRK" -> "Brooklyn Nets",
      "CHI" -> "Chicago Bulls",
      "CHO" -> "Charlotte Hornets",
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
      "POR" -> "Portland Trail Blazers",
      "SAC" -> "Sacramento Kings",
      "SAS" -> "San Antonio Spurs",
      "TOR" -> "Toronto Raptors",
      "UTA" -> "Utah Jazz",
      "WAS" -> "Washington Wizards"
    )

    println("\n------------------------------------------------------\n")

    coach_win_career_percent(sc)

    println("\n------------------------------------------------------\n")

    coach_win_current_career(sc)

    println("\n------------------------------------------------------\n")

    coach_clutch(sc, team_map)

  }

  def coach_win_career_percent(sc: SparkContext) {
    println("coach name, team name, win % this season, career win %")
    // 0: Name of Coach,
    // 1: Team,
    // 2: Games this season,
    // 3: Wins this season,
    // 11: Career win %
    sc.textFile("coaches.csv").map(x => {
      val tokens = x.split(",")
      (tokens(0), (tokens(1), tokens(3).toDouble / tokens(2).toInt, tokens(11).toDouble))
      // sort by difference in career percentage to season percentage
    }).sortBy(x => {
      Math.abs(x._2._3 - x._2._2)
    }).collect().foreach(x => {
      val coachName = x._1
      val teamName = x._2._1
      val seasonWin = x._2._2
      val careerWin = x._2._3
      printf("\n%10s, %3s, %.2f, %.2f", coachName, teamName, seasonWin, careerWin)
    })
  }

  def coach_win_current_career(sc: SparkContext): Unit ={
    // 0: Name of Coach,
    // 1: Team,
    // 2: Games this season,
    // 3: Wins this season,
    // 4: loses this season
    // 8: career games
    println("coach, team, win-lose in season, games in career")
    sc.textFile("coaches.csv").map(x => {
      val tokens = x.split(",")
      (tokens(0), (tokens(1), tokens(3) + "-" + tokens(4), tokens(8).toInt))
    }).sortBy(x => {
      x._2._3 * -1
    }).collect().foreach(x => {
      val coachName = x._1
      val coachTeam = x._2._1
      val wins_season = x._2._2
      val wins_career = x._2._3
      printf("\n%s, %3s, %s, %d", coachName, coachTeam, wins_season, wins_career)
    })
  }

  // what is the record for the coach who has the best record in games decided by 3 or less points
  def coach_clutch(sc: SparkContext, team_map: Map[String, String]) {
    println("coach name, 3 point difference season record, season record")
    val map_team = for ((k,v) <- team_map) yield (v, k)
    // 0. Team Name,
    // 1 Overall record,
    // 2 home record,
    // 3 road record, 4 record vs east, 5 record vs west, 6 record vs atlantic, 7 record vs central, 8 record vs southeast,
    // 9 record vs northwest, 10 record vs pacific, 11 record vs southwest, 12 record before all-star break,
    // 13 record post all-start break,
    // 14 record in games decided by 3 or less points
    val best_record = sc.textFile("standings.csv").map(x => {
      val tokens = x.split(",")
      val three_pts_record = tokens(14)
      val record = three_pts_record.split("-")
      record(0).toInt - record(1).toInt
    }).max()

    val coach_record = sc.textFile("coaches.csv").map(x => {
      val tokens = x.split(",")
      (tokens(1), (tokens(0), tokens(3) + "-" + tokens(4)))
    })

    sc.textFile("standings.csv").map(x => {
      val tokens = x.split(",")
      val team_name = tokens(0)
      val three_pts_record = tokens(14)
      val record = three_pts_record.split("-")
      (team_name, (record(0).toInt - record(1).toInt, three_pts_record))
    }).filter(x => {
      x._2._1 == best_record
    }).map(x => {
      (map_team(x._1), x._2._2)
    }).join(coach_record) // (team key, (3 pts record, (coach name, record in season))
      .map(x => {
        (x._2._2._1, x._2._1, x._2._2._2)
      }) // coach name, 3 pt record, record in season
      .foreach(x => {
        val coach_name = x._1
        val three_record = x._2
        val season_record= x._3
        printf("\n%s, %s, %s\n", coach_name, three_record, season_record)
      })
  }
}
