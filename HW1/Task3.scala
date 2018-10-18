
  import org.apache.spark.{SparkConf, SparkContext}

  object Task3 {
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
      val textFile = spark_context.textFile("./survey_results_public.csv/survey_results_public.csv")
      val parse = textFile.map(splitcol)
      val header = parse.first()
      //parse.foreach(println)
      val remove_rows0 = parse.filter(row => row != header)
      val remove_rows1 = remove_rows0.filter(x => x._2 != "NA")
      val remove_rows2 = remove_rows1.filter(x => x._2 != "0")
        .map(x => (x._1.trim.replace("\"",""), x._2.trim.replace("\"","")
          .replace(",",""), x._3.trim.replace("NA","Yearly")))
      val emit_weekly = remove_rows2.filter(x => x._3 == "Weekly").map(x => (x._1, x._2.toDouble * 52, x._3))
      val emit_monthly = remove_rows2.filter(x => x._3 == "Monthly").map(x => (x._1, x._2.toDouble * 12, x._3))
      val emit_yearly = remove_rows2.filter(x => x._3 == "Yearly" ).map(x => (x._1, x._2.toDouble, x._3))
      val emit_merged = emit_weekly++emit_monthly++emit_yearly

      val grpcountry = emit_merged.map(x => (x._1,(x._2,1))).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2))
      val emit_max = emit_merged.map(x => (x._1,x._2)).reduceByKey((a,b) => math.max(a,b))
      val emit_min = emit_merged.map(x => (x._1,x._2)).reduceByKey((a,b) => math.min(a,b))
      val res0 = emit_min.join(emit_max).map(x => (x._1, (x._2._1,x._2._2)))
      val res = grpcountry.join(res0).map(x => (x._1,x._2._1._2.toInt,x._2._2._1.toInt,x._2._2._2.toInt,BigDecimal((math rint (x._2._1._1/x._2._1._2)*100)/100).bigDecimal))
      val res1 = res.collect().sorted
//      for(x <- res1){
//        print(x._1+","+x._2+","+x._3+","+x._4+","+x._5+"\n")
//      }
      import java.io._
      val pw = new PrintWriter(new File("./Task3.csv" ))
      for(x <- res1){
        pw.write(x._1+","+x._2+","+x._3+","+x._4+","+x._5+"\n")
      }
      pw.close()
    }
  }

