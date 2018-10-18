
import org.apache.spark.{SparkConf, SparkContext}

object Task7 {

  def main(args: Array[String]): Unit = {
    val spark_Conf= new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val spark_context=new SparkContext(spark_Conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("avg")
      .getOrCreate
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./survey_results_public.csv/survey_results_public.csv").select( "Country", "Salary","SalaryType")
    val textFile =df.rdd
    val remove_rows0 = textFile.map(x => (x.get(0),x.get(1),x.get(2))).filter(x => x._2 != "NA")
    val remove_rows2 = remove_rows0.filter(x => x._2 != "0")
    val remove_rows1 = remove_rows2.map(x => (x._1,x._2.toString.replace(",",""),x._3))
    //val remove_rows2 = remove_rows1.map(x =>  x._3.replace("NA","Yearly"))

    val emit_weekly = remove_rows1.filter(x => x._3 == "Weekly").map(x => (x._1, x._2.toString.toDouble * 52, x._3))
    val emit_monthly = remove_rows1.filter(x => x._3 == "Monthly").map(x => (x._1, x._2.toString.toDouble * 12, x._3))
    val emit_yearly = remove_rows1.filter(x => x._3 == "Yearly" || x._3 == "NA" ).map(x => (x._1, x._2.toString.toDouble, x._3))
    val emit_merged = emit_weekly++emit_monthly++emit_yearly

    val grpcountry = emit_merged.map(x => (x._1.toString,(x._2.toString.toDouble,1))).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).sortBy(_._1)
    val emit_max = emit_merged.map(x => (x._1.toString,x._2.toString.toDouble)).reduceByKey((a,b) => math.max(a,b))
    val emit_min = emit_merged.map(x => (x._1.toString,x._2.toString.toDouble)).reduceByKey((a,b) => math.min(a,b))
    val res0 = emit_min.join(emit_max).map(x => (x._1, (x._2._1,x._2._2)))
    val res = grpcountry.join(res0).map(x => (x._1,x._2._1._2.toInt,x._2._2._1.toInt,x._2._2._2.toInt,(math rint (x._2._1._1/x._2._1._2)*100)/100))
    val res1 = res.collect().sorted

    import java.io._
    val pw = new PrintWriter(new File("./Task33.csv" ))
    for(x <- res1){
      pw.write(x._1+","+x._2+","+x._3+","+x._4+","+x._5+"\n")
    }
    pw.close()
  }
}

