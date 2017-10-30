import java.util

import org.apache.spark.rdd.RDD
import java.util.{HashMap => JavaHashMap}

import hebe.Hebe.Settings

import scala.collection.JavaConversions._

/**
  * Created by xupeng5 on 2017/7/31.
  */
class ForMat {
	
	//读取featuremap文件到HashMap
	def readFeatureMapFile(feaRDD:RDD[String]) : JavaHashMap[String,String] = {
		val feamap = new JavaHashMap[String,String]
		val tmpCollect = feaRDD.collect()
		
		for (fea <- tmpCollect) {
			val tokens = fea.toString.split("\t")
			val key = tokens(0).toString + "_" +tokens(1).toString
			val value = tokens(2).toString
			feamap.put(key,value)
		}
		feamap
	}
	
	
	//将原始数据进行编号
	def  feaFormat(inputRDD:RDD[String],feaMapRDD:RDD[String],settings: Settings): RDD[String] = {
		val feaList:JavaHashMap[String,String] = readFeatureMapFile(feaMapRDD)
		var resRDD:RDD[String] = null
		if(settings.getFormatList.head.getPattern.toString == "libsvm"){
			val colnmn:Array[String] = settings.getPartialList.head.getColumns.split(",")
			resRDD = inputRDD.map { line =>
				val tokens = line.split("\t")
				var resStr = tokens(0) + "\t"
				var count:Int = 0
				for (token <- tokens) {
					if(count>0){
						val key = colnmn(count).toString+"_"+token
						resStr += (feaList.get(key) + ":1\t")
					}
					count += 1
				}
				resStr.substring(0,resStr.size-1)
			}
			//resRDD.take(1000).foreach(x=>println(x))
		}
		resRDD
	}
}
