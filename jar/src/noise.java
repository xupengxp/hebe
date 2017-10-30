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

//import javax.swing.text.html.HTMLDocument.Iterator;
import java.lang.Math;
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
import java.util.Random;
import java.lang.Math;

public class noise {
	public static String column_names = "";
	public static String column_indices = "";
	public static String column_to = "";
	public static String pv_index = "0", clk_index = "0";
    public static String column_value = "", factor = "";

	public static void Init(String path) throws IOException, DocumentException {
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

    	List<Noise> transform = settings.getNoiseList();
    	for (int i=0; i<transform.size(); i++) {
    		Noise trans = transform.get(i);
    		String[] trans_columns = trans.getColumns().split(",");
    		String[] trans_to = trans.getTo().split(",");
    		for (int k=0; k<trans_columns.length; k++) {
    			int ind = columns.indexOf(trans_columns[k]);
    			if (ind != -1) {
    				column_names += columns.get(ind).trim() + ",";
    				column_indices += String.valueOf(ind) + ",";
    				column_to += trans_to[k] + ",";
    			}
    		}
            column_value = trans.getValue();
            factor = trans.getFactor();
    	}
        if (!column_names.equals("")) {
        	column_names = column_names.substring(0, column_names.length()-1);
        	column_indices = column_indices.substring(0, column_indices.length()-1);
        	column_to = column_to.substring(0, column_to.length()-1);
        } else {
            column_names = "null";
            column_indices = "null";
            column_to = "null";
        }

    	System.out.println("column_names: " + column_names);
    	System.out.println("column_indices: " + column_indices);
    	System.out.println("column_to: " + column_to);
		System.out.println("pv_index: " + pv_index);
		System.out.println("factor: " + factor);
        System.out.println("column_value: " + column_value);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text attr = new Text();
		private Text pv_click = new Text();

		private static String[] column_names;
		private static int[] column_indices;
		private static String[] column_to;
        private static float column_factor;
        private static String[] column_value;
		private static int pv_index, clk_index;

		public void setup(Context context) {
            String v = context.getConfiguration().get("column_names");
            String[] column_indices_string;
            if (!v.equals("null")) {
			    column_names = context.getConfiguration().get("column_names").split(",", 0);
			    column_indices_string = context.getConfiguration().get("column_indices").split(",", 0);
                column_to = context.getConfiguration().get("column_to").split(",", 0);
                column_value = context.getConfiguration().get("column_value").split(",",0);
                
                column_factor = Float.parseFloat(context.getConfiguration().get("factor"));
    
            } else {
                column_names = new String[0];
                column_indices_string = new String[0];
                column_to = new String[0];
            }
			column_indices = new int[column_indices_string.length];
			for (int i=0; i<column_indices_string.length; i++)
				column_indices[i] = Integer.parseInt(column_indices_string[i]);
			
			pv_index = Integer.parseInt(context.getConfiguration().get("pv_index"));
			clk_index = Integer.parseInt(context.getConfiguration().get("clk_index"));

			System.out.println("column_names size " + column_names.length);
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] attributes = line.split("\001", -1);

			//System.out.println("attributes: " + attributes.length + ", columns: " + column_names.length);
			//int start = column_indices[0];

			//构造加入编号的新的记录
			StringBuilder s = new StringBuilder();
			StringBuilder a = new StringBuilder();
			// s.append(attributes[CLICK]+" |");
			for (int i=0; i<attributes.length; i++) {
				s.append(attributes[i]);
				s.append('\001');
			}
            String _value;

			for (int i=0; i<column_names.length; i++) {
                int ind = column_indices[i];
                if (Math.random() > column_factor)
                    _value = attributes[ind];
                else
                    _value = column_value[i];
                s.append(_value);
			    s.append('\001');
			}
            
			// 去掉最后一个"\001"
			s.deleteCharAt(s.length()-1);
			attr.set(s.toString());
			pv_click.set("");
			context.write(attr, pv_click);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) {
			try {

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String columns = otherArgs[1], to = otherArgs[2], value = otherArgs[3], factor = otherArgs[4];
		Init(otherArgs[0]);

		conf.set("column_names", column_names);
		conf.set("column_indices", column_indices);
		conf.set("column_to", column_to);
		conf.set("pv_index", pv_index);
		conf.set("clk_index", clk_index);
        conf.set("column_value", column_value);
        conf.set("factor", factor);

		Job job = new Job(conf, "noise");
		job.setJarByClass(noise.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置reduce个数为0
		job.setNumReduceTasks(0);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[5]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[6]));

		job.waitForCompletion(true);
	}
}
