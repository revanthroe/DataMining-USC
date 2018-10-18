 import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
 import org.apache.spark.{SparkConf, SparkContext}

 import scala.collection.mutable.HashMap

  object ModelBasedCF {
    def main(args: Array[String]) {
      val spark_Conf= new SparkConf().setAppName("Recommend").setMaster("local[2]")
      val spark_context=new SparkContext(spark_Conf)
      val trainFile = spark_context.textFile(args(0))
      val testFile = spark_context.textFile(args(1))
      val hTrain = trainFile.first()
      val hTest = testFile.first()
      val trainData = trainFile.filter(row => row!= hTrain)
      val testData = testFile.filter(row => row!= hTest)
      val trainData1 = trainData.map(_.split(',') match { case Array(user, business, rate) => (user,business,rate)})
      val testData1 = testData.map(_.split(',') match { case Array(user, business, rate) => (user,business,rate)})

      val user_id = trainData1.map(x=>x._1).union(testData1.map(x=>x._1)).distinct().collect()

      var count: Int = 1
      val user_map = new HashMap[String, Integer]
      val user_map_dup = new HashMap[Integer, String]

      user_id.foreach { user =>
          user_map.put(user,count)
          user_map_dup.put(count,user)
          count = count + 1
        }

      val business_id= trainData1.map(x=>x._2).union(testData1.map(x=>x._2)).distinct().collect()
      var count11: Int = 1
      val business_map = new HashMap[String, Integer]
      val business_map_dup = new HashMap[Integer, String]

      business_id.foreach { business =>
          business_map.put(business,count11)
          business_map_dup.put(count11,business)
          count11 = count11 + 1
      }

      val ratings = trainData1.map{ case (user, business, rate) =>
        Rating(user_map.get(user).head.toInt, business_map.get(business).head.toInt, rate.toDouble)
      }



      val ratings1 = testData1.map{ case (user, business, rate) =>
        Rating(user_map.get(user).head.toInt, business_map.get(business).head.toInt, rate.toDouble)
      }
      val ratings11 = testData1.map{ case (user, business, rate) =>
        ((user_map.get(user).head.toInt, business_map.get(business).head.toInt), rate.toDouble)
      }

            val rank = 3
            val numIterations = 22 // BEST (3,23,0.27) b=3,s=1
            val lambda = 0.28 //3,22,0.29
            //val alpha = 0.01
            val model = ALS.train(ratings, rank, numIterations, lambda,3,0) // add ,3,1

            val usersProducts = ratings1.map { case Rating(user, product, rate) => (user, product) }

            val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
              ((user, product), rate)
            }

            val ratesAndPreds = ratings1.map { case Rating(user, product, rate) =>
              ((user, product), rate)
            }.join(predictions)

            val ratesAndPreds1= ratesAndPreds.map{ case ((user, business),(rate1, rate2)) =>
            ((user_map_dup.get(user).head, business_map_dup.get(business).head), (rate1.toDouble,rate2.toDouble))
            }

            val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
              val err = r1 - r2
              err * err
            }.mean()
            val rmse = Math.sqrt(MSE)


      val difference = ratesAndPreds.map{case ((user, product),(r1, r2)) =>((user, product), Math.abs(r1 - r2))}

      val count1 = difference.filter(x => x._2 >= 0 && x._2 <1).count()
      val count2 = difference.filter(x => x._2 >= 1 && x._2 <2).count()
      val count3 = difference.filter(x => x._2 >= 2 && x._2 <3).count()
      val count4 = difference.filter(x => x._2 >= 3 && x._2 <4).count()
      val count5 = difference.filter(x => x._2 >= 4 && x._2 <=5).count()

      print(">= 0 and < 1: " + count1 +"\n")
      print(">= 1 and < 2: " + count2 +"\n")
      print(">= 2 and < 3: " + count3 +"\n")
      print(">= 3 and < 4: " + count4 +"\n")
      print(">= 4 and <= 5: " + count5 +"\n")
      print("RMSE:" + rmse)

     import java.io._
      val pw = new PrintWriter(args(2))
      pw.append("User_id"+","+"Business_id"+","+"Rating"+"\n")
      val ratesAndPreds2 = ratesAndPreds1.sortByKey().collect()
      for(x <- ratesAndPreds2)
        pw.append(x._1._1+","+x._1._2+","+x._2._2+"\n")
      pw.close()
    }
  }


