
/**
  * Created by xupeng5 on 2017/7/31.
  */

import java.util

import scala.collection.JavaConversions._
import hebe.Hebe.Settings
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Partial {
	//读取hdfs文件到rdd，根据columns筛选所需字段
	def getPartial(setting:Settings,hdfsPath:String,sc:SparkContext) : RDD[String] =  {
		var  oriRDD = sc.textFile(hdfsPath)
		
		val columnsList = setting.getPartialList.head.getColumns.split(",")
		val selectArr:util.ArrayList[Int] = new util.ArrayList[Int]()
		for (column <- columnsList) {
			selectArr.add(run.getIndex(column))
		}
		
		oriRDD = oriRDD.map { line =>
			val token = line.split("\\x01")
			var res:String = ""
			var count = 0
			for (column <- columnsList){
				res += (token(selectArr.get(count)) + "\t")
				count += 1
			}
			res.substring(0,res.size-1)
		}
		run.allColumns = setting.getPartialList.head.getColumns
		//oriRDD = oriRDD.repartition(600)
		oriRDD
	}
}
