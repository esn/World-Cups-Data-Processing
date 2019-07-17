
// in case there is a SparkContext already running, please stop it

// Query11
def executeQ11RDD(): Unit = {
import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf()
.setAppName("Query11")
.setMaster("local[*]")
conf.set("spark.driver.allowMultipleContexts", "true");
val sc = new SparkContext(conf)
val Countries = sc.textFile("countries.csv")
val CountriesheaderAndRows = Countries.map(rdd => rdd.split(","))
val Teams = sc.textFile("teams.csv")
val TeamsheaderAndRows = Teams.map(line => line.split(","))
import java.util.Arrays
val team = TeamsheaderAndRows.map(a => (a{1},a))
val country = CountriesheaderAndRows.map(a => (a{0},a))
val joined = team.join(country).collect
val normalizeee = joined.map(record => (record._1, record._2._1.mkString(","), record._2._2.mkString(",")))
val normalize = joined.map(record => (record._2._1.mkString(","),record._2._2.mkString(","))).distinct
val bla = normalize.map(x => x._1.concat(",".concat(x._2)))
val ljolj = bla.map(rdd => rdd.split(","))
val bu = ljolj.map(a => (a{0},(a{1},a{2},a{4})))
val nu = bu.map(a => (a._1,Array(a._2)))
val paral = sc.parallelize(bu)
val Players = sc.textFile("players.csv")
val PlayersheaderAndRows = Players.map(line => line.split(","))
val player = PlayersheaderAndRows.map(a => (a{5},a))
val joins = paral.join(player)
val njomp = joins.map(record => (record._2._1.toString,record._2._2.mkString(",")))
val bljuc = njomp.map(x => x._1.concat(",".concat(x._2)))
val pisa = bljuc.map(x => x.replaceAll("[()]", ""))
val idemoDalje = pisa.map(x=>x.split(","))
val jajeMoje = idemoDalje.map(a => (a{1},(a{2},a{3},a{7})))
val Groups = sc.textFile("groups.csv")
val GroupsheaderAndRows = Groups.map(line => line.split(","))
val group = GroupsheaderAndRows.map(a => (a{0},a))
val joinosss = group.join(jajeMoje)
val norma = joinosss.map(x => (x._2._1.mkString(","),x._2._2.toString))
val neznam = norma.map(x => x._1.concat(",".concat(x._2)))
val kitas = neznam.map(x => x.replaceAll("[()]", ""))
val njama = kitas.map(x => x.split(","))
val jajence = njama.map(a => (a{5},(a{2},a{3},a{4})))
val Clubs = sc.textFile("clubs.csv")
val ClubsheaderAndRows = Clubs.map(line => line.split(","))
val club = ClubsheaderAndRows.map(a => (a{0},a))
val joining = club.join(jajence)
val uskoroGotovo = joining.map(a => (a._2._1.mkString(","),a._2._2.toString))
val con = uskoroGotovo.map(x => x._1.concat(",".concat(x._2)))
val oh = con.map(x => x.replaceAll("[()]", ""))
val pickica = oh.map(x => x.split(","))
val juhu = pickica.map(x => (x{1},x{3},x{4},x{5}))
val concatinating = juhu.map(x => x._1.concat(",".concat(x._2.concat(",".concat(x._3.concat(",".concat(x._4)))))))
val josMalkice = concatinating.map(x => x.split(","))
val FinalHeader = Array("Cl_name","tyear","Co_name","pid")
val FinalData = josMalkice.filter(_ != FinalHeader)
val Filter = FinalData.filter(x => x.contains("Germany"))
val Filte = Filter.filter(map => map.contains("2014"))
val pls = Filte.map(x => x.mkString(","))
val Query11 = pls.map(_.split(",")).keyBy(_(0)).groupByKey().map{case (k, v) => (k, v.size)}.collect.sortWith(_._2 > _._2)
Query11.foreach(println)
}


//running
executeQ11RDD

//Query12
def executeQ12RDD(): Unit = {
import org.apache.spark.{SparkConf, SparkContext}
val conf = new SparkConf()
.setAppName("Query12")
.setMaster("local[*]")
conf.set("spark.driver.allowMultipleContexts", "true");
val sc = new SparkContext(conf)
val Countries = sc.textFile("countries.csv")
val CountriesheaderAndRows = Countries.map(rdd => rdd.split(","))
val country = CountriesheaderAndRows.map(a => (a{0},a))
val Hosts = sc.textFile("hosts.csv")
val HostsheaderRows = Hosts.map(rdd => rdd.split(","))
val host = HostsheaderRows.map(a => (a{1},a))
val join = country.join(host)
val normalize = join.map(record => (record._2._1.mkString(","), record._2._2.mkString(",")))
val concate = normalize.map(x => x._1.concat(",".concat(x._2)))
val filterHeader = concate.filter(x => !x.contains("cid"))
val trying = filterHeader.map(line => line.split(","))
val moveAround = trying.map(line => (line{2},line{1}))
val HostsAndCountries = moveAround.map(x => x._1.concat(",".concat(x._2)))
val splitting = HostsAndCountries.map(x => x.split(","))
val leftSide = splitting.map(a => (a{0},a))
val Matches = sc.textFile("matches.csv") 
val Matches2 = Matches.mapPartitionsWithIndex {(idx, iter) => if (idx == 0) iter.drop(1) else iter }
val MatchesheaderAndRows = Matches2.map(rdd => rdd.split(",")) 
val matchiz = MatchesheaderAndRows.map(a => (a{5})) 
val matchesss = matchiz.map(x => (x.slice(0,4),x)) 
val concat = matchesss.map(x => x._1.concat(",".concat(x._2))).distinct 
import java.time.LocalDate 
import java.time.format.DateTimeFormatter 
implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay) 
val radi = concat.map(_.split(",")).keyBy(_(0)).groupByKey().map{case (k, v) => { 
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd") 
val parsedDates = v.map(sa => LocalDate.parse(sa(1), formatter)) 
(k, parsedDates.max.getDayOfYear - parsedDates.min.getDayOfYear) }}
val ma = radi.map(x => x._1.concat(",".concat(x._2.toString)))
val splitujemo = ma.map(x => x.split(","))
val rightSide = splitujemo.map(a => (a{0},a))
val finalJoin = leftSide.join(rightSide)
val normalization = finalJoin.map(record => (record._2._1.mkString(","), record._2._2.mkString(",")))
val josMalo = normalization.map(line => line._1.concat(",".concat(line._2)))
val joMalo = josMalo.map(rdd => rdd.split(","))
val finalQuery12 = joMalo.map(elem => (elem{0},elem{1},elem{3}))
val ok = finalQuery12.sortBy(x => (x._3, x._1))
val Query12 = ok.map(x => (x._1.concat(",".concat(x._2.concat(",".concat(x._3)))))).collect
Query12.foreach(println)
}

//running
executeQ12RDD