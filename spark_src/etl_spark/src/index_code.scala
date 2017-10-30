import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xupeng5 on 2017/7/21.
  */

class feature_map{
	def index_feamap(input:RDD[String],index:Int) : RDD[String] = {
		val rdd = input.map{x =>
			val tokens = x.split("\t")
			tokens(index)
		}.distinct().sortBy(x => x)
		rdd
	}
}


object index_code {
	
	//agrs : input_path output_path
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("xupeng5 spark job").set("spark.akka.frameSize", "128").set("spark.driver.maxResultSize", "3g")
		val sc = new SparkContext(conf)
		val input = args(0)
		val output = args(1)
		val code = new feature_map()
		val rdd = sc.textFile(input)
		val fea_rdd = code.index_feamap(rdd,1)
		fea_rdd.saveAsTextFile(output)
	}
}
