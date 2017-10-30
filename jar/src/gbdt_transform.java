import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;
import java.util.Iterator; 
import java.util.ArrayList;  
import java.io.FileInputStream;
import java.lang.String;
import java.util.Properties; 
import java.util.Vector;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;

//import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.*;
import hebe.Hebe;
import hebe.Hebe.DataPartial;
import hebe.Hebe.Noise;
import hebe.Hebe.Transform;
import hebe.Hebe.Format;
import hebe.Hebe.Filter;
import hebe.Hebe.Settings;
import hebe.Hebe.Settings.Builder;

public class gbdt_transform {
	//子树子节点
	public static class node_t
	{
		public int node_id;        //子树下节点node的id
		public int left;           //左子树  left_id 
		public int right;          //右子树  right_id
		public float impurity;     //infomation_gain  
		public int featureMap_idx; //featureMapID  交叉特征ID
		public float threshold;    //阀值
		public int feature_type;   //特征类型 feature_type(0 value/1 category)
		//public ArrayList<int> value_list  category_list
		public float value;        // 叶子节点的值  value(only leaf valid) 
		public int feature_idx = 0; //feature_ID 离散后的特征ID
	}

	public static String column_names = "";
	public static String column_indices = "";
	public static String column_to = "";
	public static String pv_index = "0", clk_index = "0";  
    public static String offset = "0"; //特征偏移量 
    public static String column_format = "";

	//初始化阶段,加载配置文件
	public static void Init(String path, String _offset) throws IOException, DocumentException 
	{
		//获得模型特征偏移量
		System.out.println("offset: " + _offset);
		offset = _offset;

        //处理配置文件
		FileReader fp = new FileReader(new File(path));
		Settings.Builder builder = Settings.newBuilder();
		TextFormat.merge((Readable)fp, (com.google.protobuf.Message.Builder)builder);
		Settings settings = builder.build();

		// 从protobuf中解析：特征（列）名称，所在索引，pv/clk索引
		ArrayList<String> columns = new ArrayList<String>();
		List<DataPartial> partial = settings.getPartialList();
		for (int i=0; i<partial.size(); i++) {
			DataPartial part = partial.get(i);
			String[] part_columns = part.getColumns().split(",");
			for (int k=0; k<part_columns.length; k++) {
				if (columns.indexOf(part_columns[k]) == -1)
					columns.add(part_columns[k]);
			}
		}
		
		for (int i=0; i<columns.size(); i++) {
			if (columns.get(i).equals("pv"))
				pv_index = String.valueOf(i);
			if (columns.get(i).equals("click"))
				clk_index = String.valueOf(i);
		}

        List<Noise> noise = settings.getNoiseList();
        for (int i=0; i<noise.size(); i++) {
            Noise item = noise.get(i);
            String[] to_columns = item.getTo().split(",");
            for (int k=0; k<to_columns.length; k++) {
                if (columns.indexOf(to_columns[k]) == -1)
                    columns.add(to_columns[k]);
            }
        }
		List<Transform> transform = settings.getTransformList();
		for (int i=0; i<transform.size(); i++) {
			Transform trans = transform.get(i);
			String[] trans_to = trans.getTo().split(",");
			
			for (int k=0; k<trans_to.length; k++) {
				if (columns.indexOf(trans_to[k]) != -1) {
					columns.add(trans_to[k].trim());
				}
			}
		}

		// filter节点的input格式为 f1:1,f2:1,9:g1,10:g2,11:g3...，代表f1和f2为离散之后的特征，g1/g2/g3为数值特征，9/10/11为特征名,f1/f2后的1为特征值，如果gbdt判定的特征找不到默认为0
		List<Filter> filter = settings.getFilterList();
		for (int i=0; i<filter.size(); i++) 
		{
			Filter flt = filter.get(i);
			column_format = flt.getInputs();
			String[] flt_inputs = flt.getInputs().split(",");
			String flt_outputs_str = flt.getOutputs();
			String[] flt_outputs;
			if (flt_outputs_str.indexOf("%d") != -1) {
				int num_output = flt.getOutputNum();
				flt_outputs = new String[num_output];
				String output_name = flt_outputs_str.substring(0, flt_outputs_str.indexOf("%d"));
				for (int k=0; k<num_output; k++) {
					flt_outputs[k] = output_name + String.valueOf(k);
				}
			}
			else
				flt_outputs = flt.getOutputs().split(",");

			for (int k=0; k<flt_inputs.length; k++) {
				String[] tmp = flt_inputs[k].trim().split(":");
				int ind = columns.indexOf(tmp[0]);
				if (ind == -1)
					ind = columns.indexOf(tmp[1]);
    			if (ind != -1) {
    				column_names += columns.get(ind).trim() + ",";
    				column_indices += String.valueOf(ind) + ",";
    			}
			}
			for (int k=0; k<flt_outputs.length; k++) {
				column_to += flt_outputs[k].trim() + ",";
			}
		}
		
		if (!column_names.equals("")) {
			column_names = column_names.substring(0, column_names.length()-1);
			column_indices = column_indices.substring(0, column_indices.length()-1);
			column_to = column_to.substring(0, column_to.length()-1);
		} 
		else {
			column_names = "null";
			column_indices = "null";
			column_to = "null";
		    column_format = "null";
		}
		
		System.out.println("column_names: " + column_names);
		System.out.println("column_indices: " + column_indices);
		System.out.println("column_to: " + column_to);
		System.out.println("column_format: " + column_format);

		System.out.println("pv_index: " + pv_index);
		System.out.println("clk_index: " + clk_index);
		System.out.println("offset: " + offset);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		private Text _key = new Text();
        private Text _value = new Text();

		private String[] column_names;
		private int[] column_indices;
		private String[] column_to;
		private String column_format;
		private int pv_index, clk_index;
		private int offset = 0;

		//子树索引
		public int tree_idx = 0;
		//子树个数
		public int treeCount = 0;
		//子树子节点个数
		private HashMap<Integer, HashMap<Integer, node_t>> tree_node_hash = new HashMap<Integer, HashMap<Integer, node_t>>();
	    private HashMap<Integer, Integer> features = new HashMap<Integer, Integer>();
		
		//map初始化阶段,加载模型文件
		public void setup(Context context) 
		{
			String v = context.getConfiguration().get("column_names");
			String[] column_indices_string;
			if (!v.equals("null")) {
				column_names = context.getConfiguration().get("column_names").split(",", 0);
				column_indices_string = context.getConfiguration().get("column_indices").split(",", 0);
				column_to = context.getConfiguration().get("column_to").split(",", 0);
			    column_format = context.getConfiguration().get("column_format");
			} else {
				column_names = new String[0];
				column_indices_string = new String[0];
				column_to = new String[0];
				column_format = "";
			}
			
			column_indices = new int[column_indices_string.length];
			for (int i=0; i<column_indices_string.length; i++)
				column_indices[i] = Integer.parseInt(column_indices_string[i]);
			
			pv_index = Integer.parseInt(context.getConfiguration().get("pv_index"));
			clk_index = Integer.parseInt(context.getConfiguration().get("clk_index"));
			offset = Integer.parseInt(context.getConfiguration().get("offset")); //获得偏移量

			try 
			{
				// xgboost_model.ini
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				int feature_offset = offset;
				
				if (null != cacheFiles && cacheFiles.length > 0) 
				{
					System.out.println("load xgboost file: " + cacheFiles[0].toString());
					
					BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));

					String line = null;
					String[] array = null;
					String[] content = null;
					while ((line = reader.readLine()) != null)
					{
						//[tree_0]
						//if (line.matches("\\[.*\\]")) //加载子树编号
						if (line.startsWith("[tree_")) //		
						{
							//获得子树编号
							tree_idx = Integer.valueOf(line.replaceFirst("\\[(.*)\\]", "$1").split("_")[1]);
							 
							//设置子树对应的节点哈希表
							HashMap<Integer, node_t> node_hash = new HashMap<Integer, node_t>();
							tree_node_hash.put(tree_idx, node_hash);
							
						    //记录子树数量
							treeCount++;
						}
						//node_0=1,2,null,4,5316,0,,0
						//else if ((!line.trim().startsWith("#")) && line.matches(".*=.*")) //加载子树节点编号 
						else if (line.startsWith("node_")) //加载子树节点编号		
						{
							array = line.split("=");
							content = array[1].split(",");
							 
							node_t tree_node = new node_t();
							tree_node.node_id = Integer.valueOf(array[0].split("_")[1]); //子树下节点node的id
							tree_node.left = Integer.valueOf(content[0]);    // 左子树  left_id 
							tree_node.right = Integer.valueOf(content[1]);   // 右子树  right_id
							
							//如果是叶子节点则进行特征编号
							if ((tree_node.left == -1) && (tree_node.right == -1))
							{
								//如果是叶子节点则进行特征编号
								tree_node.feature_idx = feature_offset;

								//偏移量递增
								feature_offset++;
							//	System.out.println("tree_node.feature_idx: " + tree_node.feature_idx);	
							}
							
							//tree_node.impurity = Float.valueOf(content[2]); // infomation_gain  
							tree_node.featureMap_idx = Integer.valueOf(content[3]); // feature ID 交叉特征ID
							tree_node.threshold = Float.valueOf(content[4]);      // 阀值
							tree_node.feature_type = Integer.valueOf(content[5]); //特征类型 feature_type(0 value/1 category)
							//public ArrayList<int> value_list  //category_list
							tree_node.value = Float.valueOf(content[7]);  //叶子节点的值 value(only leaf valid)         
							
							//每课子树下增加子节点
							if (!(tree_node_hash.get(tree_idx).containsKey(tree_node.node_id)))
							{
								tree_node_hash.get(tree_idx).put(tree_node.node_id, tree_node);
							}
						}
					}
					reader.close();

					//输出模型数据以便测试 
				/*	for(Entry<Integer, HashMap<Integer, node_t>> entry : tree_node_hash.entrySet())
					{
						System.out.print("tree_" + entry.getKey() + " ");
						                                         
						for (Entry<Integer, node_t> entry_sub : entry.getValue().entrySet())
						{
							System.out.print("node_" + entry_sub.getKey() + " ");
							System.out.print("node_id " + entry_sub.getValue().node_id + " ");//子树下节点node的id
							System.out.print("left " + entry_sub.getValue().left + " ");      //左子树 left_id 
							System.out.print("right " + entry_sub.getValue().right + " ");    //右子树 right_id
							System.out.print("featureMap_idx " + entry_sub.getValue().featureMap_idx + " "); //featureMapID 交叉特征ID
							System.out.print("threshold " + entry_sub.getValue().threshold + " ");  //阀值
							System.out.print("feature_type " + entry_sub.getValue().feature_type + " "); //特征类型 feature_type(0 value/1 category)
							System.out.print("value " + entry_sub.getValue().value + " ");       // 叶子节点的值  value(only leaf valid) 
							System.out.println("feature_idx " + entry_sub.getValue().feature_idx); // feature_ID 离散后的特征ID         
						}                   
					}  */
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}

			//System.out.println("column_names size " + column_names.length);
			//System.out.println("feature_map_file has " + hash_index.size() + " lines");
		}

		//处理每一条数据
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] attributes = line.split("\001", -1);

			int max_index = 0;
			for (int i=0; i<column_indices.length; i++) 
			{
				if (max_index < column_indices[i])
					max_index = column_indices[i];
			}
			
			if (max_index >= attributes.length) 
			{
				System.out.println("wrong line! " + attributes.length);
				return;
			}

            String feature_value = column_format; 
			String[] array = new String[column_names.length];
			for (int i=0; i<column_names.length; i++) 
			{
			    feature_value = feature_value.replace(column_names[i], attributes[column_indices[i]].trim());
				//array[i] = attributes[column_indices[i]];
			}
	
            features.clear(); 
			String[] values = feature_value.split(",");
			for (int i=0; i<values.length; i++) {
			    String[] _t = values[i].split(":");
				features.put(Integer.parseInt(_t[0].trim()), Integer.parseInt(_t[1].trim()));
			}
            //System.out.println("features: " + feature_value + " [" + features.size() + "]");

			//# node_id=left_id, right_id, impurity(infomation gain), feature_id/feature_name, thresh, feature_type(0 value/1 category), value_list(category list), value(only leaf valid)
			//[tree_0]
			//node_0=1,2,0.04815729865260272,6,617.0,0,,-0.975624262381475
			ArrayList<Integer> vec = new ArrayList<Integer>();

			int leaf_index = 0;
			int feature_index = 0;
			// [10, 20, 30, 40, 50, 60]		
			HashMap<Integer, node_t> node_map;		
			//循环每一棵树，计算落到叶子节点的个数
			for (int tree_index = 0; tree_index < this.treeCount; tree_index++)
			{		
				leaf_index = 0;
				feature_index = 0;
				//得到每棵树的节点数据
				node_map = tree_node_hash.get(tree_index);
				while ((node_map.get(leaf_index).left != -1) && (node_map.get(leaf_index).right != -1))
				{
					//获得模型使用特征的id
					feature_index = node_map.get(leaf_index).featureMap_idx;
					float v = 0;
					if (features.containsKey(feature_index))
						v = features.get(feature_index);
					if (v < node_map.get(leaf_index).threshold)
						leaf_index = node_map.get(leaf_index).left;
					else
						leaf_index = node_map.get(leaf_index).right;
				}																																				
				vec.add(node_map.get(leaf_index).feature_idx);
			}	
			if (vec.size() != column_to.length) {
				System.out.println("wrong gbdt encode! " + vec.size() + " != " + column_to.length);
				return;
			}

			//将模型计算结果写入到结果中
			StringBuilder s = new StringBuilder();
			for (int i=0; i<attributes.length; i++) {
				s.append(attributes[i]);
				s.append('\001');
			}

			for (int i=0; i<vec.size(); i++) {
				s.append(String.valueOf(vec.get(i)));
				s.append('\001');
			}
			// 去掉最后一个"\001"
			s.deleteCharAt(s.length()-1);

			_key.set(s.toString());
			_value.set("");
			context.write(_key, _value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) {
			try {
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		/*  配置文件、输入、输出、模型文件、偏移量
			hadoop jar $HEBE/jar/hebe.jar feature_transform -libjars $HEBE/jar/protobuf-java-2.4.2.jar 
				/data0/result/chenglei/hebe/test//conf/config.conf 配置文件  0
				$WAREHOUSE/hebetest/shrink_data  输入数据 1
				$WAREHOUSE/hebetest/whole_data   输出数据 2
            	$WAREHOUSE/hebetest/xgboost.ini  模型文件 3
				offset 偏移量 4*/
			
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Init(otherArgs[0], otherArgs[4]); //加载配置文件 config.ini 和偏移量  

		conf.set("column_names", column_names);
		conf.set("column_indices", column_indices);
		conf.set("column_to", column_to);
		conf.set("pv_index", pv_index);
		conf.set("clk_index", clk_index);
		conf.set("offset", offset);
        conf.set("column_format", column_format);

		String path = otherArgs[3]; //xgboost模型文件
		System.out.println(new Path(path).toUri());
		DistributedCache.addCacheFile(new Path(path).toUri(), conf);

		Job job = new Job(conf, "gbdt_transform");
		job.setJarByClass(gbdt_transform.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置reduce个数为0
		job.setNumReduceTasks(0);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));   //数据输入路径
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); //数据输出路径

		job.waitForCompletion(true);
	}
}
