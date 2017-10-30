package AdOwner_Cluster;

import java.io.BufferedReader; 
import java.io.FileReader; 
import java.io.IOException; 
import java.util.HashMap; 
import java.util.Properties; 
import java.util.Map.Entry;
import java.util.Vector;

public class IniReader 
{ 
	//子树子节点
	public class node_t 
	{
		public int node_id;     // 子树下节点node的id
		public int left;        // 左子树    array[0]  left_id 
		public int right;       // 友子树    array[1]  right_id
		public float impurity;  // infomation_gain array[2]  
		public int featureMap_idx; // featureMapID  array[3] 交叉特征ID
		public float threshold;    // 阀值   array[4]
		public int feature_type;   //特征类型  array[5]  feature_type(0 value/1 category)
		//public ArrayList<int> value_list  //category_list array[6]
		public float value;       // 叶子节点的值   array[7] value(only leaf valid) 
		public int feature_idx = 0; // feature_ID 离散后的特征ID
	}
	
	//子树索引
	private int tree_idx; 
	//子树个数
	private int	treeCount = 0;	
	//子树子节点个数
	private HashMap<Integer, HashMap<Integer, node_t>> tree_node_hash = new HashMap<Integer, HashMap<Integer, node_t>>(); 
	//
	private String feature_offset = "10000";
	//
	private int feature_offset_index = 0;
		
	public IniReader(String filename) throws IOException 
	{ 
		this.tree_node_hash.clear();
		
		if (!feature_offset.isEmpty())
		{
			feature_offset_index = Integer.parseInt(feature_offset);
		}
				
		BufferedReader reader = new BufferedReader(new FileReader(filename)); 
		read(reader); 
		reader.close(); 
		
		for(Entry<Integer, HashMap<Integer, node_t>> entry : tree_node_hash.entrySet())
		{
			System.out.print("tree_" + entry.getKey() + " ");
			
			for (Entry<Integer, node_t> entry_sub : entry.getValue().entrySet())
			{
				System.out.print("node_" + entry_sub.getKey() + " ");
				
				System.out.print("node_id " + entry_sub.getValue().node_id + " ");  // 子树下节点node的id
				System.out.print("left " + entry_sub.getValue().left + " ");     // 左子树    array[0]  left_id 
				System.out.print("right " + entry_sub.getValue().right + " ");    // 友子树    array[1]  right_id
				System.out.print("featureMap_idx " + entry_sub.getValue().featureMap_idx + " "); // featureMapID  array[3]  交叉特征ID
				System.out.print("threshold " + entry_sub.getValue().threshold + " ");  // 阀值   array[4]
				System.out.print("feature_type " + entry_sub.getValue().feature_type + " "); //特征类型  array[5]  feature_type(0 value/1 category)
				System.out.print("value " + entry_sub.getValue().value + " ");       // 叶子节点的值   array[7] value(only leaf valid) 
				System.out.println("feature_idx " + entry_sub.getValue().feature_idx); // feature_ID 离散后的特征ID			
			}					
		}	
	}
	
	//扫描每一行的数据
	protected void parseLine(String line) 
	{ 
		//[tree_0]
		if (line.matches("\\[.*\\]")) //加载子树编号
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
		else if ((!line.trim().startsWith("#")) && line.matches(".*=.*")) //加载子树节点编号 
		{ 
		    String[] array = line.split("=");
			String[] content = array[1].split(",");
			
			node_t tree_node = new node_t();
			tree_node.node_id = Integer.valueOf(array[0].split("_")[1]);   // 子树下节点node的id
			tree_node.left = Integer.valueOf(content[0]);    // 左子树    content[0]  left_id 
			tree_node.right = Integer.valueOf(content[1]);   // 友子树    content[1]  right_id
			
			if ((tree_node.left == -1) && (tree_node.right == -1))
			{
				//偏移量递增
				feature_offset_index++;
				
				//如果是叶子节点则进行特征编号
				tree_node.feature_idx = feature_offset_index;
			}
			
			//tree_node.impurity = Float.valueOf(content[2]); // infomation_gain content[2]  
			tree_node.featureMap_idx = Integer.valueOf(content[3]); // feature ID  content[3]  交叉特征ID
			tree_node.threshold = Float.valueOf(content[4]);      // 阀值   content[4]
			tree_node.feature_type = Integer.valueOf(content[5]); //特征类型  content[5]  feature_type(0 value/1 category)
			//public ArrayList<int> value_list  //category_list content[6]
			tree_node.value = Float.valueOf(content[7]);  //叶子节点的值   content[7] value(only leaf valid)			
			
			//每课子树下增加子节点
			if (!(tree_node_hash.get(tree_idx).containsKey(tree_node.node_id)))
			{
				tree_node_hash.get(tree_idx).put(tree_node.node_id, tree_node);				
			}			
		}
	}

	//读取模型文件
	protected void read(BufferedReader reader) throws IOException 
	{ 
		String line; 
		while ((line = reader.readLine()) != null) 
		{ 
			parseLine(line.trim()); 
		}			
	}
	
	//传入样本数据，进行判断，哪些树的叶子节点为1
	public Vector<Integer> getResult(String[] data) 
	{
		Vector<Integer> vec = new Vector<Integer>();	
			
		// [10, 20, 30, 40, 50, 60]		
		int leaf_index = 0; //每颗子树的叶子节点		
		HashMap<Integer, node_t> node_map;		
		//循环每一棵树，计算落到叶子节点的个数
		for (int tree_index = 0; tree_index < this.treeCount; tree_index++)
		{		
			leaf_index = 0;
			
			//得到每棵树的节点数据
			node_map = tree_node_hash.get(tree_index);
			
			while ((node_map.get(leaf_index).left != -1) && (node_map.get(leaf_index).right != -1))
			{
				//获得模型使用特征的id
				int feature_index = node_map.get(leaf_index).featureMap_idx;
				
				if (Float.valueOf(data[feature_index]) < node_map.get(leaf_index).threshold)
			    	leaf_index = node_map.get(leaf_index).left;
			    else
			        leaf_index = node_map.get(leaf_index).right;			
			}
			
			//String ss = node_map.get(leaf_index).node_id + " " + node_map.get(leaf_index).feature_idx;
			//vec.add(node_map.get(leaf_index).node_id);
			vec.add(node_map.get(leaf_index).feature_idx);
		}		
		
		//System.out.println(vec);		
		return vec;	
	}
	
	/*	line = line.trim(); 
	if (line.matches("\\[.*\\]")) 
	{ 
		currentSecion = line.replaceFirst("\\[(.*)\\]", "$1"); 
		current = new Properties(); 
		sections.put(currentSecion, current); 
	} 
	else if (line.matches(".*=.*")) 
	{ 
		if (current != null) 
		{ 
			int i = line.indexOf('='); 
			String name = line.substring(0, i); 
			String value = line.substring(i + 1); 
			current.setProperty(name, value); 
		} 
	}  */
	
	/*	int tree_count = 0;
	int subNode_count = 0;
	
	for(Entry<String, Properties> entry : sections.entrySet())
	{
		tree_count++;
		//System.out.println(entry.getKey());
		//System.out.println(entry.getValue().size());
		
		subNode_count = 0;
		//leafNode_count = 0;			
		for(Entry<Object, Object> e : entry.getValue().entrySet())
		{
			subNode_count++;				
		}			
		
		//计算子节点个数
		subNode_hash.put(entry.getKey(), subNode_count);		
	}
	
	System.out.println("count: " + tree_count);
	//记录子树个数
	this.treeCount = tree_count;		
	
 	System.out.println("subNode data: ");
	for (Entry<String, Integer> e : subNode_hash.entrySet())
	{
		System.out.println("subNode data: " + e.getKey() + " " + e.getValue());		
	} */	

	/*  获得每一棵树对应节点的信息数据
	public String getValue(String tree_index, String node_index) 
	{ 
		Properties p = (Properties)sections.get(tree_index); 

		if (p == null) 
		{ 
			return null; 
		}
		
		String value = p.getProperty(node_index); 
		return value; 
	}  */
		
		/* vector<node_t>& tree = gbdt_tree[i];
		
		int leaf_index = 0;
		while (tree[leaf_index].left != -1 && tree[leaf_index].right != -1) 
		{
			int feature_index = tree[leaf_index].feature;
		    if (features[feature_index] <= tree[leaf_index].thresh)
		    	leaf_index = tree[leaf_index].left;
		    else
		        leaf_index = tree[leaf_index].right;
		}  */
	
	/*	String[] array = null;				
		String left_id = "";          // array[0] left_id 
		String right_id = "";         // array[1] right_id 
		//double infomation_gain = 0.0; // array[2] impurity(infomation gain) 
		int feature_id = 0;           // array[3] feature_id/feature_name 
		double thresh = 0.0;          // array[4] thresh
		String feature_type = "";     // array[5] feature_type(0 value/1 category) 
		//ArrayList<int>              // array[6] value_list(category list) 
		double value = 0.0;           // array[7] value(only leaf valid)
		int current_node_sum = 0;		
	
		int preIndex = -1;
		int feature_offset_index = 0; */
		
//		if (!feature_offset.isEmpty())
//		{
//			feature_offset_index = Integer.parseInt(feature_offset);
//		}
		
	/*	for (;;)
		{
		//	array = getValue("tree_" + tree_index, "node_" + node_index).split(",");
			
			System.out.print(array.length + " ");
			for (String e : array)
			{
				System.out.print(e + " ");					
			}
			System.out.println();  
			
			//数据举例:  8 63 64 null 6 339 0  0 
			left_id = array[0];                         // array[0]   left_id 
			right_id = array[1];                        // array[1]   right_id 
			//double infomation_gain = 0.0;             // array[2]   impurity(infomation gain) 
			feature_id = Integer.parseInt(array[3]);    // array[3]   feature_id/feature_name, 
			thresh = Double.parseDouble(array[4]);      // array[4]   thresh
			feature_type = array[5];                    // array[5]  feature_type(0 value/1 category), 
			//ArrayList<int> value_list(category list), // array[6]
			value = Double.parseDouble(array[7]);       // array[7]   value(only leaf valid)	

			//获得下一个节点的id
			if (Double.parseDouble( tree_node_hash.get(tree_index).    data[feature_id]) < thresh)
			{
				//设置下一个要计算的节点
				node_index = Integer.parseInt(left_id);
			}
			else
			{
				//设置下一个要计算的节点
				node_index = Integer.parseInt(right_id);					
			}
			
			//System.out.println(getValue("tree_" + tree_index, "node_" + node_index).split(",")[0]);
			//System.out.println(getValue("tree_" + tree_index, "node_" + node_index).split(",")[1]);
			
			array = getValue("tree_" + tree_index, "node_" + node_index).split(",");
			
			//如果是叶子节点则退出
			if (array[0].equalsIgnoreCase("-1") && array[1].equalsIgnoreCase("-1"))
			{
				break;
			}
		}		
		
		//获得每一棵树的叶子节点数
		if (tree_index == 0)
		{
			//记录扫描所有当前叶子节点的个数
			node_index = (node_index + 1);
		}
		else
		{
			//获得前一棵树的子节点数
			preIndex = tree_index - 1;
			current_node_sum += subNode_hash.get("tree_" + preIndex);
			
			//得到当前子树的索引
			node_index = (current_node_sum + node_index + 1);								
		}		
		//		
		vec.add(feature_offset_index + node_index);			
	}
	
	/*
	for(Entry<String, Properties> entry : sections.entrySet())
	{
		count++;
		System.out.println(entry.getKey());
		System.out.println(entry.getValue().size());
		
		for(Entry<Object, Object> e : entry.getValue().entrySet())
		{
			System.out.println(e.getKey());
			System.out.println(e.getValue());				
		}		
	} */	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException 
	{		
		IniReader reader = new IniReader("./file/online_xgboost.ini");
		
		String[] data = {"10", "20", "30", "40", "50", "60", "70"};
		Vector vec = reader.getResult(data);
	    System.out.println(vec.size());	
				
	/*	int i =5;
		Vector vec = new Vector();
		vec.add(i);		
		i = 7;
		vec.add(i);		
		i = 9;
		vec.add(i);		
		System.out.println(vec); */
		
		//hashINI("./file/config.ini");
				//System.out.println(getHashValue("Section1","Key1"));
				//System.out.println(getHashValue("Section1","Key2"));
				//System.out.println(getHashValue("Section2","Key"));
				
			/*	[TestSect1] 
				kkk 1=VVVVVVVVVVV1 
				kkk 2=VVVVVVVVVVV2 

				[TestSect2] 
				kkk 3=VVVVVVVVVVV3 
				kkk 4=VVVVVVVVVVV4 

				[TestSect3] 
				kkk 5=VVVVVVVVVVV5 
				kkk 6=VVVVVVVVVVV6  */
		
//		System.out.println(reader.getValue("TestSect1", "kkk 1"));
//		System.out.println(reader.getValue("TestSect1", "kkk 2"));
//		
//		System.out.println(reader.getValue("TestSect2", "kkk 3"));
//		System.out.println(reader.getValue("TestSect2", "kkk 4"));
//		
//		System.out.println(reader.getValue("TestSect3", "kkk 5"));
//		System.out.println(reader.getValue("TestSect3", "kkk 6"));
	    
	  //# node_id=left_id, right_id, impurity(infomation gain), feature_id/feature_name, thresh, feature_type(0 value/1 category), value_list(category list), value(only leaf valid)
	  		//[tree_0]
	  		//node_0=1,2,0.04815729865260272,6,617.0,0,   ,-0.975624262381475
	  			
	  	/*	typedef struct {
	          	int node_id;
	          	int left, right;
	          	float impurity;
	          	int feature;
	          	float thresh, value;    // only leaf node has value
	         } node_t;	  
	  	  
	  	   for (int i=0; i<gbdt_tree.size(); i++) {
	  			vector<node_t>& tree = gbdt_tree[i];
	  			 
	  			int leaf_index = 0;
	  			while (tree[leaf_index].left != -1 && tree[leaf_index].right != -1) {
	  				int feature_index = tree[leaf_index].feature;
	  			    if (features[feature_index] <= tree[leaf_index].thresh)
	  			    	leaf_index = tree[leaf_index].left;
	  			    else
	  			        leaf_index = tree[leaf_index].right;
	  			}
	  			 
	  		//printf("tree:%d, leaf:%d\n", i, leaf_index);
	  		int code = code_table[i][leaf_index];
	  		leaf_indexs.push_back(code);  */
	}
}