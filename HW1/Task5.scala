import org.apache.spark.{SparkConf, SparkContext}

object Task5 {

  def main(args: Array[String]): Unit = {
    val spark_Conf= new SparkConf().setAppName("Count").setMaster("local[2]")
    val spark_context=new SparkContext(spark_Conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("country_count")
      .getOrCreate
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./survey_results_public.csv/survey_results_public.csv").select( "Country", "Salary")
    val textFile =df.rdd
    val remove_rows0 = textFile.map(x => (x.get(0),x.get(1))).filter(x => x._2 != "NA")
    val remove_rows1 = remove_rows0.filter(x => x._2 != "0")
    val emit = remove_rows1.map(x => (x._1.toString,1)).reduceByKey(_+_).sortBy(_._1).collect()
    val tot = spark_context.parallelize(emit)
    val res = tot.values.sum().toInt
    val res1 = "Total"+","+res

    import java.io._
    val pw = new PrintWriter(new File("./Task11.csv" ))
    pw.write(res1+"\n")
    for(x <- emit){
      pw.append(x._1+","+x._2+"\n")
    }
    pw.close()
  }
}
