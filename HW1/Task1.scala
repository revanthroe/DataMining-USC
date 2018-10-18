import org.apache.spark.{SparkConf, SparkContext}

object Task1 {
  //Function to get the required columns from the data set.
  def splitcol(line:String)={
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    val country = fields(3)
    val salary = fields(52)
    val salary_type = fields(53)
    (country,salary,salary_type)
  }

  def main(args: Array[String]): Unit = {
    val spark_Conf= new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val spark_context=new SparkContext(spark_Conf)
    val textFile = spark_context.textFile("./survey_results_public.csv/survey_results_public.csv",2)
    //val count = textFile.flatMap(line => line.split(',')).map(word => (word, 1)).reduceByKey(_ + _)
    val parse = textFile.map(splitcol)
    val header = parse.first()
    //parse.foreach(println)
    //println(parse.count())
    val remove_rows0 = parse.filter(row => row != header)
    val remove_rows1 = remove_rows0.filter(x => x._2 != "NA")
    val remove_rows2 = remove_rows1.filter(x => x._2 != "0")
    //filter.foreach(println)
    //println(remove_rows2.count())
    val emit = remove_rows2.map(x =>(x._1.trim.replace("\"",""), 1 ))
      .reduceByKey((a,b) => a+b)
      .sortByKey(true).collect()
    val tot = spark_context.parallelize(emit)
    val res = tot.values.sum().toInt
    val res1 = "Total"+","+res
    println(res1)
    for(x <- emit){
      println(x._1+","+x._2)
    }
    import java.io._
    val pw = new PrintWriter(new File("./Task1.csv" ))
    pw.write(res1+"\n")
    for(x <- emit){
      pw.append(x._1+","+x._2+"\n")
    }
    pw.close()
  }
}
