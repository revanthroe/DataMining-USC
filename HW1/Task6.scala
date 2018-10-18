import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Task6 {

  def main(args: Array[String]): Unit = {
    val spark_Conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val spark_context = new SparkContext(spark_Conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("partitions")
      .getOrCreate
    val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./survey_results_public.csv/survey_results_public.csv").select( "Country", "Salary")
    val textFile =df.rdd
    //val myRows = textFile.map(line => { (line(3),line(52),line(53))
    val remove_rows1 = textFile.map(x => (x.get(0),x.get(1))).filter(x => x._2 != "NA")
    val remove_rows2 = remove_rows1.filter(x => x._2 != "0")
    val textFile1 = remove_rows2.coalesce(2)
    val res = textFile1.mapPartitions(iter => Iterator(iter.size), true).collect()
    val t0 = System.currentTimeMillis()
    val reduce1 = textFile1.map(x => (x._1,1)).reduceByKey(_+_)
    val t1 = System.currentTimeMillis()
    var l1  = List[Int]()
    for (x <- res){
      l1::=x
    }

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

    import java.io._
    val pw = new PrintWriter(new File("./Task22.csv" ))
    pw.write("standard,"+l1(0)+","+l1(1)+","+(t1-t0)+"\n")
    pw.append("partition,"+l2(0)+","+l2(1)+","+(t4-t3)+"\n")
    pw.close()
  }
}
