package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    standings.createOrReplaceTempView("standingsView")
    standings.sqlContext.sql("select season, league, avg(goalsFor) from standingsView group by season, league order by season, league").show()

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    val bestTeam = standings
      .filter($"league" === "Ligue 1" && $"position" === 1)
      .groupBy($"team")
      .count().as("count")
      .orderBy(desc("count"))
      .head().getAs[String]("team")
    println(s"L'équipe française la plus titrée est $bestTeam.\n")

    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    standings
      .filter($"position" === 1)
      .groupBy($"league")
      .avg("points")
      .show()

    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val decade: Int => String = _.toString.init + "X"
    val decadeUdf = udf(decade)

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")
    standings
      .select($"league", $"season", $"position", $"team", $"points")
      .filter($"position" === 1 || $"position" === 10)
      .withColumn("decade", decadeUdf('season))
      .groupBy($"decade", $"league")
      .pivot("position")
      .agg(avg($"points"))
      .withColumn("average gap", $"1" - $"10")
      .drop($"1").drop($"10")
      .sort($"decade", $"league")
      .show()

  }
}
