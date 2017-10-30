import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import hebe.Hebe.Settings
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

import scala.util.Random



/**
  * Created by xupeng5 on 2017/7/25.
  */

class Sample {
	
	//从settings组合sql条件
	def shrinkConditionParse(settings: Settings) : String = {
		var resStr = ""
		
		for (shrink <- settings.getShrinkList){
			for (con <- shrink.getArgs.split(" ")){
				val conList = con.split(":")
				if (conList.size == 3) {
					resStr += ("(" + conList(0) + "=" + conList(1) + " and " + conList(0) + "_rand<" + conList(2) + ") or ")
				}else {
					println("error shrink conition!")
				}
			}
		}
		resStr
	}
	
	
	def sam(settings: Settings,spark: SparkSession) : DataFrame = {
		val partial = settings.getPartialList.head
		val talbleName = partial.getName
		val columnsName = partial.getColumns
		val shrink = settings.getShrinkList
		
		import spark.implicits._
		
		//初始化table,且增加采样字段的随机列
		var sqlStr = "select " + columnsName
		for (item <- shrink){
			sqlStr += ",rand() as " + item.getArgs.split(" ").head.split(":").head + "_rand"
		}
		sqlStr += (" from " + talbleName+" where dt=20170727")

		//得到整张表及需要shrink字段的rand数
		var shrinkDf = spark.sql(sqlStr)
		
		var conSql = shrinkConditionParse(settings)
		conSql = conSql.substring(0,conSql.size - 3)
		println(conSql)
		shrinkDf = shrinkDf.where(conSql)
		shrinkDf
		
	}
	
	def getFilter(settings: Settings) :Array[String] = {
		val shrink = settings.getShrinkList
		var resStr = ""
		for (_shrink <- shrink) {
			val cons = _shrink.getArgs
			val con = cons.split(" ")
			for (_con <- con) {
				resStr += (_con + ",")
			}
		}
		resStr = resStr.substring(0,resStr.size-1)
		println("bbbbb"+resStr)
		resStr.split(",")
	}
	
	def samRDD(settings: Settings, sc:SparkContext,rdd:RDD[String]): RDD[String] = {
		var random = (new Random())
		val dataPath = settings.getPartialList.head.getName
//		val rdd = sc.textFile("hdfs://ns1/user/ads/warehouse/xupeng5_promotiom_his_ctr_table/dt=20170727/")
		
		val conArr : Array[String] = getFilter(settings)
		
		val index:Int = run.getIndex(conArr.head.split(":").head.toString)
		println("aaaaaaaaa"+index.toString)
		var resRDD  = rdd.filter{ line =>
			val token = line.split("\t")
			val rand:Float = random.nextFloat()
			token(index).toString == conArr.head.split(":")(1) && rand < conArr.head.split(":")(2).toFloat
		}
		var count = 1
		for (con <- conArr) {
			if (count > 1){
				val index:Int = run.getIndex(con.split(":")(0))
				if (index == -1){
					println("error column name!!!")
				} else {
					val tmpRDD = rdd.filter { line =>
						val token = line.split("\t")
						val rand: Float = random.nextFloat()
						token(index).toString == con.split(":")(1) && rand < con.split(":")(2).toFloat
					}
					resRDD = resRDD.union(tmpRDD)
				}
			}
			count += 1
		}
		
		resRDD
	}
}





	
