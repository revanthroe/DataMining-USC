import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Task2 {
  def splitcol(line:String)={
    val fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
    val country = fields(3)
    val salary = fields(52)
    val salary_type = fields(53)
    (country,salary,salary_type)
  }

  def main(args: Array[String]): Unit = {
    val spark_Conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val spark_context = new SparkContext(spark_Conf)
    val textFile = spark_context.textFile("./survey_results_public.csv/survey_results_public.csv",2)
    val parse = textFile.map(splitcol)
    val header = parse.first()
    val remove_rows0 = parse.filter(row => row != header)
    val remove_rows1 = remove_rows0.filter(x => x._2 != "NA")
    val remove_rows2 = remove_rows1.filter(x => x._2 != "0")

    val textFile1 = remove_rows2.coalesce(2)
    val res = textFile1.mapPartitions(iter => Iterator(iter.size), true).collect()
    val t0 = System.currentTimeMillis()
    val reduce1 = textFile1.map(x => (x._1,1)).reduceByKey(_+_)
    val t1 = System.currentTimeMillis()
    //res.foreach(println)
    var l1  = List[Int]()
    for (x <- res){
      l1::=x
    }
    //println(t1-t0)

    val textFile2 = remove_rows2.keyBy(x => x._1).partitionBy(new HashPartitioner(2))
    val res2 = textFile2.mapPartitions(iter => Iterator(iter.size), true).collect()
    val t3 = System.currentTimeMillis()
    val reduce2 = textFile2.map(x => (x._1,1)).reduceByKey(_+_)
    val t4 = System.currentTimeMillis()
    //res2.foreach(println)
    var l2  = List[Int]()
    for (x <- res2){
      l2::=x
    }
    //println(t4-t3)

    import java.io._
    val pw = new PrintWriter(new File("./Task2.csv" ))
    pw.write("Standard,"+l1(0)+","+l1(1)+","+(t1-t0)+"\n")
    pw.append("partition,"+l2(0)+","+l2(1)+","+(t4-t3)+"\n")
    pw.close()
  }
}
