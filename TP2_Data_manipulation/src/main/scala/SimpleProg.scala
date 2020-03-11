/* SimpleProg.scala */
import org.apache.spark.sql.SparkSession

object SimpleProg {
  def main(args: Array[String]) {
    val myFile = "file:///home/hadoop/msata/cnam_RCP216/data_mining/TP2_Data_manipulation/data/tpSparkScala.html"
    val spark = SparkSession.builder.appName("Application Simple").getOrCreate()
    val myData = spark.read.textFile(myFile).cache()
    val nbclass = myData.filter(line => line.contains("class")).count()
    val nbjavascript = myData.filter(line => line.contains("javascript")).count()
    println("Lignes avec javascript : %s, lignes avec class : %s".format(nbjavascript, nbclass))
    spark.stop()
  }
}
