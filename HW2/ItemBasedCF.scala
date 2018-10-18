
import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashMap

object ItemBasedCF {
  def main(args: Array[String]) {
    val start_time = System.nanoTime()
    val spark_Conf = new SparkConf().setAppName("Recommend").setMaster("local[2]")
    val spark_context = new SparkContext(spark_Conf)
    val trainFile = spark_context.textFile(args(0))
    val testFile = spark_context.textFile(args(1))
    val hTrain = trainFile.first()
    val hTest = testFile.first()
    val trainData = trainFile.filter(row => row != hTrain)
    val testData = testFile.filter(row => row != hTest)
    val trainDataRDD = trainData.map(_.split(',') match { case Array(user, business, rate) => (user, business, rate) })
    val testDataRDD = testData.map(_.split(',') match { case Array(user, business, rate) => (user, business, rate) })

    val user_id = trainDataRDD.map(x=>x._1).distinct().collect()

    var count: Int = 1
    val user_map = new HashMap[String, Integer]
    val rev_user_map = new HashMap[Integer, String]

    user_id.foreach { user =>
      user_map.put(user,count)
      rev_user_map.put(count,user)
      count = count + 1
    }

    val business_id= trainDataRDD.map(x=>x._2).distinct().collect()

    var count11: Int = 1
    val business_map = new HashMap[String, Integer]
    val rev_business_map = new HashMap[Integer, String]

    business_id.foreach { business =>
      business_map.put(business,count11)
      rev_business_map.put(count11,business)
      count11 = count11 + 1
    }

    val trainDataSource = trainDataRDD.map{ case (user, business, rate) =>
      (user_map.get(user).head.toInt, business_map.get(business).head.toInt, rate.toDouble)
    }

    val tmpbratingMatrix = trainDataSource.groupBy(_._2).map(ra=>{
      (ra._1,ra._2.map(buss =>(buss._1,buss._3)))
    }).map(ra =>{
      (ra._1,ra._2.toMap)
    })

    val orderByBusiness = tmpbratingMatrix.collectAsMap()


    val rating00 = trainDataSource.map { case (user, product, rate) =>((user.toInt,product.toInt),rate.toDouble)}
    val rating01 = trainDataSource.map { case (user, product, rate) =>(user.toInt,product.toInt)}
    val rating02 = trainDataSource.map { case (user, product, rate) =>(product.toInt,user.toInt)}


    /*val testDataSource = testDataRDD.map{ case (user, business, rate) =>
      ((user_map.get(user).head.toInt, business_map.get(business).head.toInt),rate.toDouble)
    }*/
    //FIND USER AVERAGES
    //val user_avg_final = user_avg.map(x => (user_map_dup(x._1),x._2))
    //user_avg.foreach(println)

   var cosineSimilarity = new mutable.HashMap[Integer, mutable.HashMap[Integer,Double]]()
    val tmpratingMatrix = trainDataSource.groupBy(_._1).map(ra=>{
      (ra._1,ra._2.map(buss =>(buss._2,buss._3)))
    }).map(ra =>{
      (ra._1,ra._2.toMap)
    })

    val orderByUser = tmpratingMatrix.collectAsMap()

//    //      FIND USER AVERAGES
//    val user_rdd = trainDataRDD.map{case(user, product,rate_text) => (user_map.get(user).head.toInt, rate_text.toDouble)}.groupByKey.mapValues(_.toList)
//    val user_avg = user_rdd.map(x => (x._1,x._2.sum/x._2.size))
//    val user_avg_map = new HashMap[Integer, Double]
//    user_avg.foreach(row => {
//      user_avg_map.put(row._1,row._2)
//    })
//
//    //FIND BUSINESS AVERAGE
//    val business_rdd = trainDataRDD.map{case(user, product,rate_text) => (business_map.get(product).head.toInt, rate_text.toDouble)}.groupByKey.mapValues(_.toList)
//    val business_avg = business_rdd.map(x => (x._1,x._2.sum/x._2.size))
//    val business_avg_map = new HashMap[Integer, Double]
//    business_avg.foreach(row => {
//      business_avg_map.put(row._1,row._2)
//    })

    //Normalized ratings
    //val norm = user_avg.join(rating01).map(x => ((x._1,x._2._2._1),x._2._2._2 - x._2._1))

    //val grp_by_business = rating02.groupByKey().collectAsMap()
    //val grp_by_user = rating01.groupByKey().collectAsMap()
//    val ratingsuser = trainData1.map{ case (user, business, rate) =>
//      ((user_map.get(user).head.toInt, business_map.get(business).head.toInt), rate.toDouble)
//    }.collectAsMap()


    var finalRatings : List[(String,String,String,Double)] = List()
    testDataRDD.collect().foreach(y => {
      if(user_map.contains(y._1) && ! business_map.contains(y._2)){
        val x = orderByUser.get(user_map.get(y._1).head).head.keySet
        var avg = 0.00
        x.foreach(r=>{
         avg = avg + orderByUser.get(user_map.get(y._1).head).head.get(r).head
        })
        avg = avg/x.size
        finalRatings = finalRatings :+ (y._1,y._2,y._3,avg)
      }
      else if (!user_map.contains(y._1) && business_map.contains(y._2)){
        val x = orderByBusiness.get(business_map.get(y._2).head).head.keySet
        var avg = 0.00
        x.foreach(r=>{
          avg = avg + orderByBusiness.get(business_map.get(y._2).head).head.get(r).head
        })
        avg = avg/x.size
        finalRatings = finalRatings :+ (y._1,y._2,y._3,avg)
      }
      else if(user_map.contains(y._1) && business_map.contains(y._2)){
        var prediction  = 0.00
        var numpredict = 0.0
        var denpredict = 0.0
        orderByUser.get(user_map.get(y._1).head).head.foreach(value=>{
          var similValue = 0.0
          if(cosineSimilarity.contains(value._1) && cosineSimilarity.get(value._1).head.contains(business_map.get(y._2).head)){
            similValue = cosineSimilarity.get(value._1).head.get(business_map.get(y._2).head).head
          }
          else{
            var p,q,r = 0.0
            orderByBusiness.get(value._1).head.foreach(orderBuss=>{
              if(orderByBusiness.get(business_map.get(y._2).head).head.contains(orderBuss._1)){
                p = orderBuss._2 * orderByBusiness.get(business_map.get(y._2).head).get(orderBuss._1) + p
                q = q + Math.pow(orderBuss._2,2)
                r = r + Math.pow(orderByBusiness.get(business_map.get(y._2).head).get(orderBuss._1),2)
              }
            })
            if(p!=0 && q!=0)
            similValue = p/(Math.sqrt(q)*Math.sqrt(r))
            if(cosineSimilarity.contains(value._1)){
              cosineSimilarity.get(value._1).head.put(business_map.get(y._2).head,similValue)
            } else {
              cosineSimilarity.put(value._1,new mutable.HashMap[Integer,Double])
              cosineSimilarity.get(value._1).head.put(business_map.get(y._2).head,similValue)
            }
            if(cosineSimilarity.contains(business_map.get(y._2).head)){
              cosineSimilarity.get(business_map.get(y._2).head).head.put(value._1,similValue)
            } else {
              cosineSimilarity.put(business_map.get(y._2).head,new mutable.HashMap[Integer,Double])
              cosineSimilarity.get(business_map.get(y._2).head).head.put(value._1,similValue)
            }
          }
          if(similValue!=0) {
            numpredict = numpredict + value._2 * similValue
            denpredict = Math.abs(similValue) + denpredict
          }
        })

        if(denpredict!=0)
            prediction = numpredict/denpredict
        else
          prediction = 3.0
        finalRatings = finalRatings :+ (y._1,y._2,y._3,prediction)
      }
      else {
        finalRatings = finalRatings :+ (y._1,y._2,y._3,3.0)
      }
    })
//    //Cosine Coefficient
//    ratings1.foreach(y => {
//      grp_by_business.get(y._1._2).head.foreach(x => {
//        val numerator = ratingMatrix(y._1._1,y._1._2) * ratingMatrix(x,y._1._2)
//        val denominator = Math.sqrt(ratingMatrix(y._1._1,y._1._2) * ratingMatrix(y._1._1,y._1._2)) * Math.sqrt(ratingMatrix(x,y._1._2)*ratingMatrix(x,y._1._2))
//        val cosine = numerator/denominator
//        cosine_map.put((y._1._1,x),cosine)
//      })
//    })
//    //cosine_map.foreach(println)

    var count1 = 0
    var count2 = 0
    var count3 = 0
    var count4 = 0
    var count5 = 0
    finalRatings.foreach(row => {
      val x = Math.abs(row._3.toInt - row._4)
      if (x >= 0 && x < 1) count1 = count1 + 1
      if (x >= 1 && x < 2) count2 = count2 + 1
      if (x >= 2 && x < 3) count3 = count3 + 1
      if (x >= 3 && x < 4) count4 = count4 + 1
      if (x >= 4 && x <= 5) count5 = count5 + 1
    })
    print(">= 0 and < 1: " + count1 +"\n")
    print(">= 1 and < 2: " + count2 +"\n")
    print(">= 2 and < 3: " + count3 +"\n")
    print(">= 3 and < 4: " + count4 +"\n")
    print(">= 4 and <= 5: " + count5 +"\n")

    val pw = new PrintWriter(args(2))
    pw.append("User_id"+","+"Business_id"+","+"Prediction"+"\n")
    var RMSE = 0.0
    var size = 0
    val finalRatings1 = finalRatings.map(x => ((x._1,x._2),x._3,x._4))

    finalRatings1.sorted.foreach(row=>{
      pw.append(row._1._1+","+row._1._2+","+row._3+"\n")
      RMSE = RMSE + (row._2.toDouble-row._3)*(row._2.toDouble-row._3)
      size = size + 1
    })
    RMSE = RMSE/size
    RMSE = Math.sqrt(RMSE)

    println("RMSE " + RMSE)

    pw.close()
    val end_time = System.nanoTime()
    print("The total execution time taken is " +  ((end_time - start_time) / 1000000000) + " sec.")
  }
}
