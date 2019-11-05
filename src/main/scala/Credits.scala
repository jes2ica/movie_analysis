import java.io.PrintWriter

import org.apache.spark.SparkContext
import cats.implicits._
import io.circe.parser._
import io.circe.generic.auto._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD


/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Credits {
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Create a SparkContext using every core of the local machine, named Credits
    val sc = new SparkContext("local[*]", "Credits")
    processRel(sc)
    processMovie(sc)
    processCasts(sc)
    processCrews(sc)
  }
  case class Movie(id: String, title: String, release_date: String, poster_path: String, overview: String, collection: KeyVal, genres: Vector[KeyVal], budget: String, revenue: String, rating: String)
  case class KeyVal(id: Int, name: String)
  case class Cast(cast_id: Int, character: String, credit_id: String, gender: Int, id: Int, name: String, order: Int, profile_path: String)
  case class Crew(credit_id: String, department: String, gender: Int, id: Int, job: String, name: String, profile_path: String)

  def processMovie(sc: SparkContext): Unit = {
    // Load up each line of the ratings data into an RDD
    var data = sc.textFile("/Users/jes2ica/Downloads/the-movies-dataset/movies_metadata_less.csv")
    // Extract the first row which is the header
    val header = data.first();
    // Filter out the header from the dataset
    data = data.filter(row => row != header)
    val headers = Seq("id:ID(Movie-ID)","title","release_date","poster_path","overview","collection", "genres","budget","revenue","rating",":LABEL")
    val movies:Seq[Seq[String]] = data.map(row => row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(movie => Movie(movie(5), movie(20), movie(14), movie(11), movie(9), decode[KeyVal](process(movie(1))).getOrElse(KeyVal(0, "")), decode[Vector[KeyVal]](process(movie(3))).getOrElse(Vector[KeyVal]()), movie(2), movie(15), movie(22)))
      .map(m => Seq(m.id, m.title, m.release_date, m.poster_path, m.overview, m.collection.name, getGenre(m), m.budget, m.revenue, m.rating, "Movie")).collect()
    val allRows: Seq[Seq[String]] = headers +: movies
    val csv: String = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter("movies.csv") { write(csv); close() }
  }

  def getGenre(m: Movie): String = {
    var res = ""
    for (genre <- m.genres) {
      res += genre.name
      res += "|"
    }
    res
  }

  def processCasts(sc: SparkContext): Unit = {
    // Load up each line of the ratings data into an RDD
    var data = sc.textFile("/Users/jes2ica/Downloads/the-movies-dataset/credits_less.csv")
    // Extract the first row which is the header
    val header = data.first();
    // Filter out the header from the dataset
    data = data.filter(row => row != header)
    val headers = Seq("id:ID(Talent-ID)","name","profile_path",":LABEL")
    val actors = data.map(row => row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(fields => (fields(2), decode[Vector[Cast]](process(fields(0))).getOrElse(Vector[Cast]())))
      .flatMapValues(x => x).map(actor => Seq(actor._2.id.toString, actor._2.name, actor._2.profile_path,"Talent")).collect()
    val allRows: Seq[Seq[String]] = headers +: actors
    val csv: String = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter("casts.csv") { write(csv); close() }
  }

  def processCrews(sc: SparkContext): Unit = {
    // Load up each line of the ratings data into an RDD
    var data = sc.textFile("/Users/jes2ica/Downloads/the-movies-dataset/credits_less.csv")
    // Extract the first row which is the header
    val header = data.first();
    // Filter out the header from the dataset
    data = data.filter(row => row != header)
    val headers = Seq("id:ID(Talent-ID)","name","profile_path",":LABEL")
    val crews = data.map(row => row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(fields => (fields(2), decode[Vector[Crew]](process(fields(1))).getOrElse(Vector[Crew]())))
      .flatMapValues(x => x).map(crew => Seq(crew._2.id.toString, crew._2.name, crew._2.profile_path, "Talent")).collect()
    val allRows: Seq[Seq[String]] = headers +: crews
    val csv: String = allRows.map(_.mkString(",")).mkString("\n")
    new PrintWriter("crews.csv") { write(csv); close() }
  }

  def processRel(sc: SparkContext): Unit = {
    // Load up each line of the ratings data into an RDD
    var data = sc.textFile("/Users/jes2ica/Downloads/the-movies-dataset/credits_less.csv")
    // Extract the first row which is the header
    val header = data.first();
    // Filter out the header from the dataset
    data = data.filter(row => row != header)
    val headers = Seq(":START_ID(Talent-ID)", ":TYPE", ":END_ID(Movie-ID)", "Role")
    val casts: Seq[Seq[String]] = processCast(data)
    val crews: Seq[Seq[String]] = processCrew(data)
    val allRows: Seq[Seq[String]] = headers +: casts
    val csv: String = allRows.map(_.mkString(",")).mkString("\n")
    val csv_Crews: String = csv + crews.map(_.mkString(",")).mkString("\n")
    new PrintWriter("talent_movie_rel.csv") { write(csv_Crews); close() }
  }

  def processCast(data: RDD[String]): Seq[Seq[String]] = {
    val actors = data.map(row => row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(fields => (fields(2), decode[Vector[Cast]](process(fields(0))).getOrElse(Vector[Cast]())))
      .flatMapValues(x => x)
    actors.map(actor => Seq(actor._2.id.toString, "Acts", actor._1, actor._2.character)).collect()
  }

  def processCrew(data: RDD[String]): Seq[Seq[String]] = {
    val crews = data.map(row => row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(fields => (fields(2), decode[Vector[Crew]](process(fields(1))).getOrElse(Vector[Crew]())))
      .flatMapValues(x => x)
    crews.map(crew => Seq(crew._2.id.toString, "Supports", crew._1, crew._2.job)).collect()
  }

  def process(str: String): String = {
    val s = str.replace("\"\"", "\"")
      .replaceAll("'([\\w_]*)'", "\"$1\"")
      .replaceAll("'([[\\wÀ-ÿ -.]]*)'", "\"$1\"")
      .replaceAll("'([[\\w/.]]*)'", "\"$1\"")
      .replace("None", "\"\"")
    if (s.indexOf('"') == 0) s.substring(1, s.length() - 1)
    else s
  }
}
