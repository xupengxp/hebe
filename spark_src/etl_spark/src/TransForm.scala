import java.util

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import hebe.Hebe.Settings

import scala.collection.JavaConversions._
import java.util.List
import java.util.ArrayList

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
/**
  * Created by xupeng5 on 2017/7/27.
  */

class IndexTransForm {
	
	
	//获取离散特征的去重结果
	def intervalIndex(column : String,rdd:RDD[Row],spark: SparkSession,inputRDD:RDD[String]) : RDD[String] = {
		import spark.implicits._
		
		val colrdd:RDD[String] = rdd.map{row=>
			row.getAs(column).toString
		}
		
		val resRdd = colrdd.distinct.sortBy(x=>x)
		inputRDD.union(resRdd)
		
	}
	
	//获取离散特征的去重结果
	def intervalIndex(column : String,rdd:RDD[Row],spark: SparkSession) : RDD[String] = {
		import spark.implicits._
		
		val colrdd:RDD[String] = rdd.map{row=>
			row.getAs(column).toString
		}
		
		colrdd.distinct().sortBy(x=>x)
	}
	
	//将所有特征进行递增编号
	def featCode(inputRDD:RDD[String],spark: SparkSession) : RDD[String] = {
		import spark.implicits._
		var list = new ArrayList[String]()
		var coll = inputRDD.collect()
		var count  = 0
		for (co <- coll) {
			list.add(co.toString() + "\t" + count.toString())
			count += 1
		}
		var li = list.asScala
		li.toDS().rdd
	}
	
	def trans(settings: Settings,inputRDD:RDD[Row],spark: SparkSession) : RDD[String] = {
		val transList = settings.getTransformList
		
		//目前就两种编码方式，离散和连续
		var indexRDD: RDD[String] = null
		for (trans <- transList) {
			//简单离散
			if (trans.getType == "SIMPLE_INDEX") {
				var count = 1
				val transList = trans.getColumns.split("\t")
				for (trans <- transList) {
					if (count != 1) {
						indexRDD = intervalIndex(trans, inputRDD, spark, indexRDD)
					}
					else {
						indexRDD = intervalIndex(transList.head, inputRDD, spark)
					}
					count += 1
				}
				
			}else {
				println("error type!!!")
			}
		}
		featCode(indexRDD,spark)
	}
	
	def transRDD(settings: Settings,inputRDD:RDD[String],sc:SparkContext) : RDD[String] = {
		val trans = settings.getTransformList
		var indexRddFlag : Boolean = true
		var intervalRddFlag :Boolean = true
		var resIndexRDD : RDD[String] = sc.makeRDD(Seq(""))
		var resIntervalRDD : RDD[String] = sc.makeRDD(Seq(""))
		var intervalCount : Int = 0
		for (_tran <- trans){
			//先遍历所有的INDEX特征，resIndexRDD为所有离散特征去重排序结果,resIntervalRDD为
			//所有的连续特征按照特征名分组计数结果
			if (_tran.getType.toString == "SIMPLE_INDEX") {
				val columnArray = _tran.getColumns.split(",")
				val tmpRDD = indexFea(columnArray,inputRDD,sc)
				if (indexRddFlag){
					resIndexRDD = tmpRDD
					indexRddFlag = false
				}else {
					resIndexRDD = resIndexRDD.union(tmpRDD)
				}
			} else if (_tran.getType.toString == "INTERVAL_INDEX"){
				val columnArray = _tran.getColumns.split(",")
			
				val args:String = _tran.getArgs
				intervalCount += (columnArray.size * args.split(":")(0).toInt)
				val tmpRDD = intervalFea(columnArray,args,inputRDD,sc)
				if(intervalRddFlag) {
					resIntervalRDD = tmpRDD
					intervalRddFlag = false
				}else {
					resIntervalRDD = resIntervalRDD.union(tmpRDD)
				}
			} else {
				println("error transfom type!!!")
			}
		}
		
		//离散特征编号基于连续特征基础上
		var start:Int = 1
		val tmpCollect = resIndexRDD.collect()
		val tmpList = new util.ArrayList[String]()
		for(co <- tmpCollect) {
			tmpList.add(co.toString + "\t" + (start + intervalCount).toString)
			start += 1
		}
		val tmpRDD:RDD[String] = sc.makeRDD(tmpList.toList)
		
		resIntervalRDD.union(tmpRDD)
	}
	
	def indexFea(columnArray:Array[String],inputRDD:RDD[String],sc:SparkContext) : RDD[String] = {
		var count = 1
		var index = run.getIndex(columnArray.head)
		var resRDD = inputRDD.map{line =>
			val tokens = line.split("\t")
			columnArray.head.toString+"\t"+tokens(index)
		}.distinct.sortBy(x=>x)
		
		for(column <- columnArray) {
			if(count > 1) {
				index = run.getIndex(column.toString)
				val tmpRDD = inputRDD.map { line =>
					val tokens = line.split("\t")
					column + "\t" + tokens(index)
				}.distinct.sortBy(x => x)
				resRDD = resRDD.union(tmpRDD)
			}
			count += 1
		}
		resRDD
	}
	
	
	def intervalFea(columnsList:Array[String],args:String,inputRDD:RDD[String],sc:SparkContext) : RDD[String] = {
		var index:Int = run.getIndex(columnsList.head)
		val argList:Array[String] = args.split(":")
		val freq : Int = (run.pvCount/argList(0).toInt).toInt
		var flag:Boolean = true
		var resRDD : RDD[String]=null
		var startCount:Long = 0
		
		for (column <- columnsList) {
			if(flag){
				val tmpCollect = inputRDD.map{ line =>
					val tokens = line.split("\t")
					columnsList.head.toString+"\t"+tokens(index)
				}.map((_,1)).reduceByKey(_+_).sortBy{ line=>
					val key = line.toString().split("\t")(1).split(",")(0).toInt
					key
				}.collect()
				
				var sum:Long = 0
				val tmpList:ArrayList[String] = new util.ArrayList[String]()
				for (coll <- tmpCollect) {
					val tmpStr = coll.toString().substring(1,coll.toString().size-1)
					sum += tmpStr.toString().split(",")(1).toInt
					tmpList.add(tmpStr.split(",")(0)+"\t"+(sum/freq).toInt.toString)
				}
				
				resRDD=sc.makeRDD(tmpList.toList)
				startCount += 700
				flag = false
			} else {
				index = run.getIndex(column)
				val tmpCollect = inputRDD.map{ line =>
					val tokens = line.split("\t")
					column+"\t"+(tokens(index).toFloat*100000).toInt.toString
				}.map((_,1)).reduceByKey(_+_).sortBy{ line=>
					val key = line.toString().split("\t")(1).split(",")(0).toInt
					key
				}.collect()
				
				var sum:Long = 0
				val tmpList:ArrayList[String] = new util.ArrayList[String]()
				for (coll <- tmpCollect) {
					val tmpStr = coll.toString().substring(1,coll.toString().size-1)
					sum += tmpStr.toString().split(",")(1).toInt
					tmpList.add(tmpStr.split(",")(0)+"\t"+((sum/freq).toInt+startCount).toString)
				}
				startCount += 700
				resRDD = resRDD.union(sc.makeRDD(tmpList.toList))
			}
		}
		
//		resRDD.collect().foreach(x=>println(x))
		resRDD
		
	}
	
//	def feaCode(inputRDD:RDD[String]) : Unit = {
//		var coll = inputRDD.distinct().sortBy(x=>x).collect()
//		var count = 0
//		for (item <- coll) {
//			println(item+"\t"+count.toString)
//			count += 1
//		}
//	}
	
	
}
