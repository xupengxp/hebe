/**
  * Created by xupeng5 on 2017/7/24.
  */

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.util.HashMap
import java.util.Map.Entry
import java.util.List
import java.util.Iterator
import java.util.ArrayList
import java.io.FileInputStream
import java.lang.String

import org.dom4j.DocumentException
import org.dom4j.Element
import org.dom4j.io.SAXReader
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io._
import java.util

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.TextFormat
import hebe.Hebe

import hebe.Hebe.Noise
import hebe.Hebe.DataPartial
import hebe.Hebe.Transform
import hebe.Hebe.Format
import hebe.Hebe.Filter
import hebe.Hebe.Settings
import hebe.Hebe.Settings.Builder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession



object run {
	var allColumns :String = ""
	var pvCount:Long = 0
	
	def getIndex(name : String) : Int = {
		val arr = run.allColumns.split(",")
		var index : Int = -1
		var count = 0
		for (_arr <- arr) {
			if (_arr.toString.trim == name.trim){
				index = count
			}
			count += 1
		}
		index
	}
	
	def main(args: Array[String]): Unit = {
		
		//首先解析proto文件到setting
		val fp = new FileReader(new File(args(0)));
		val builder = Settings.newBuilder();
		TextFormat.merge(fp, builder);
		val settings = builder.build();
		allColumns = settings.getPartialList.head.getColumns
		//val partial = settings.getPartialList();
		
		//var sqlStr:String = ""

		//解析partial数据源，得到hive表名及字段
		
		val partial = settings.getPartialList()
			//暂时取第一个数据源

		
		val _partial = partial.head
		val _partialName = _partial.getName()
		val _partialColumns = _partial.getColumns()
		
	
		
		val conf = new SparkConf().setAppName("etl_spark").setMaster("yarn-cluster")
		val sc = new SparkContext(conf)
		
		//拼接sql，从hive读入数据到datafram
		//val spark: SparkSession = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", "spark-warehouse").enableHiveSupport().getOrCreate()
		
		val par = new Partial()
		val hdfspath:String = settings.getHdfsPath
		val partialpath:String = settings.getPartialList.head.getName
		//val hdfsPath = "hdfs://ns1/user/ads/warehouse/xupeng5_promotiom_his_ctr_table/dt=20170727/"
		val inputPath = hdfspath + settings.getName+"/" +partialpath
		val pattialRDD:RDD[String] = par.getPartial(settings,inputPath,sc)
		
		//将输入rdd缓存
		pattialRDD.cache()
		//解决rdd cache的lazy
		pattialRDD.count()
		
		val sam = new Sample()
		val samRDD = sam.samRDD(settings,sc,pattialRDD)
		samRDD.cache()
		pvCount = samRDD.count()

		val trans = new IndexTransForm()
		val transRDD =  trans.transRDD(settings,samRDD,sc)
		transRDD.repartition(1).saveAsTextFile(hdfspath+settings.getName+"/featuremap")
		//transRDD.take(100).foreach(x=>println(x))
		
		val format = new ForMat()
		val formatRDD:RDD[String] =  format.feaFormat(samRDD,transRDD,settings)
		formatRDD.saveAsTextFile(hdfspath+settings.getName+"/" + settings.getFormatList.head.getName)
		sc.stop()
	}
}
