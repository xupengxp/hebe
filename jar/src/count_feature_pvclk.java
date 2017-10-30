import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Iterator; 
import java.util.ArrayList;  
import java.io.FileInputStream;
import java.lang.String;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
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
import hebe.Hebe.Transform;
import hebe.Hebe.Format;
import hebe.Hebe.Filter;
import hebe.Hebe.Settings;
import hebe.Hebe.Settings.Builder;
import hebe.Hebe.Noise;

public class count_feature_pvclk {

	public static String column_names = "";
	public static String column_indices = "";
	public static String pv_index = "0", clk_index = "0";

	public static boolean Init(String path) throws Exception, IOException, DocumentException {
		FileReader fp = new FileReader(new File(path));  

		//Builder builder = Settings.newBuilder();
		Settings.Builder builder = Settings.newBuilder();
		TextFormat.merge((Readable)fp, (com.google.protobuf.Message.Builder)builder);
    	Settings settings = builder.build();

    	// 从protobuf中解析：特征（列）名称，所在索引，pv/clk索引
    	ArrayList<String> columns = new ArrayList<String>();
    	List<DataPartial> partial = settings.getPartialList();
    	for (int i=0; i<partial.size(); i++) {
    		DataPartial part = partial.get(i);
    		String[] part_columns = part.getColumns().split(",", 0);
    		for (int k=0; k<part_columns.length; k++) {
    			if (columns.indexOf(part_columns[k]) == -1)
    				columns.add(part_columns[k]);
    		}
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

        String label = "";
    	List<Transform> transform = settings.getTransformList();
    	for (int i=0; i<transform.size(); i++) {
    		Transform trans = transform.get(i);
			if (!label.equals("")) {
				if (!label.equals(trans.getLabel())){
					System.out.println("exception: " + label + " != " + trans.getLabel());
					//throw new Exception("exception: " + label + " != " + trans.getLabel());
					return false;
				}
			}
    		label = trans.getLabel();
			String[] trans_columns = trans.getColumns().split(",", 0);
    		for (int k=0; k<trans_columns.length; k++) {
    			int ind = columns.indexOf(trans_columns[k]);
    			if (ind != -1) {
    				column_names += columns.get(ind).trim() + ",";
    				column_indices += String.valueOf(ind) + ",";
    			}
    		}
    	}

        if (!column_names.equals("")) {
            column_names = column_names.substring(0, column_names.length()-1);
            column_indices = column_indices.substring(0, column_indices.length()-1);
        } else {
            column_names = "null";
            column_indices = "null";
        }

    	for (int i=0; i<columns.size(); i++) {
			if (columns.get(i).equals(label))
    			clk_index = String.valueOf(i);
		}

    	System.out.println("column_names: " + column_names);
    	System.out.println("column_indices: " + column_indices);
		//System.out.println("pv_index: " + pv_index);
		System.out.println("click_index: " + clk_index);
		return true;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text attr = new Text();
		private Text pv_click = new Text();

		private static String[] column_names;
		private static int[] column_indices;
		private static int pv_index, clk_index;

		public void setup(Context context) {
            String v = context.getConfiguration().get("column_names");
            String[] column_indices_string;
            if (!v.equals("null")) {
			    column_names = context.getConfiguration().get("column_names").split(",", 0);
                column_indices_string = context.getConfiguration().get("column_indices").split(",", 0);
            }
            else {
                column_names = new String[0];
                column_indices_string = new String[0];
            }
            
			column_indices = new int[column_indices_string.length];
			for (int i=0; i<column_indices_string.length; i++)
				column_indices[i] = Integer.parseInt(column_indices_string[i]);
			//pv_index = Integer.parseInt(context.getConfiguration().get("pv_index"));
			clk_index = Integer.parseInt(context.getConfiguration().get("clk_index"));
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] attributes = line.split("\001", -1);
            //System.out.println("attributes: " + attributes.length);

			for (int i=0; i<column_indices.length; i++) {
				int index = column_indices[i];
                                if (attributes.length <= index) continue;
				attr.set(column_names[i] + "-" + attributes[index]);
				
				String vv = attributes[clk_index];
				if (vv.equals("")||vv.equals("-1")) vv = "0";
                vv.trim();
                if (!vv.equals("1")) vv = "0";
				pv_click.set("1-" + vv);

				context.write(attr, pv_click);
			}
			//System.out.println(attr.toString() + "---" + pv_click.toString());
		}
	}

	public static class Combine extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) {
            String error="";
			try {
				long pv_sum = 0;
				long click_sum = 0;
				for (Text value : values) {
                    error = value.toString();
					String[] temp = value.toString().split("-", -1);
					pv_sum += Long.parseLong(temp[0]);
					click_sum += Long.parseLong(temp[1]);
				}

				context.write(key, new Text(pv_sum + "-" + click_sum));
                if(key.toString().indexOf("sim") != -1) {
                    //System.out.println("sim:" + key.toString() );
                } 

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
                System.out.println("Exception---" + key.toString() + ":" + error);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) {
			try {
				long pv_sum = 0;
				long click_sum = 0;
				for (Text value : values) {
					String[] temp = value.toString().split("-", -1);
					pv_sum += Long.parseLong(temp[0]);
					click_sum += Long.parseLong(temp[1]);
				}
                if(key.toString().indexOf("sim") != -1) {
                    //System.out.println("sim:" + key.toString() );
                } 
				context.write(key, new Text(pv_sum + "-" + click_sum));

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
                System.out.println("Exception---" + key.toString() );
			}
		}
	}

	public static void main(String[] args) throws Exception {

		// Init("G:\\fighting_gaoyan\\data\\map_reduce_info.xml");

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (!Init(otherArgs[0]))
			return;

		conf.set("column_names", column_names);
		conf.set("column_indices", column_indices);
		//conf.set("pv_index", pv_index);
		conf.set("clk_index", clk_index);

		Job job = new Job(conf, "count_feature_pvclk");
		job.setJarByClass(count_feature_pvclk.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combine.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(200);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}
}
