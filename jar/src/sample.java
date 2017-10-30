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
import java.util.Random;
import java.lang.Math;

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
import hebe.Hebe.Transform;
import hebe.Hebe.Format;
import hebe.Hebe.Filter;
import hebe.Hebe.Settings;
import hebe.Hebe.Settings.Builder;

public class sample {

    public static String column_names = "";
    public static String column_indices = "";
    public static String pv_index = "0", clk_index = "0";
    public static String names = "";
    public static String values = "";
    public static String weights = "";

    public static void Init(String path, String[] args) throws IOException, DocumentException {
        FileReader fp = new FileReader(new File(path));

        Settings.Builder builder = Settings.newBuilder();
        TextFormat.merge((Readable)fp, (com.google.protobuf.Message.Builder)builder);
        Settings settings = builder.build();

        // 从protobuf中解析：特征（列）名称，所在索引，pv/clk索引
        ArrayList<String> columns = new ArrayList<String>();
        int count = 0;
        List<DataPartial> partial = settings.getPartialList();
        for (int i=0; i<partial.size(); i++) {
            DataPartial part = partial.get(i);
            String[] part_columns = part.getColumns().split(",");
            for (int k=0; k<part_columns.length; k++) {
                if (columns.indexOf(part_columns[k]) == -1) {
                    columns.add(part_columns[k]);
                    column_names += part_columns[k].trim() + ",";
                    column_indices += String.valueOf(count) + ",";
                    count += 1;
                }
            }
        }
        for (int i=0; i<columns.size(); i++) {
            if (columns.get(i).equals("pv"))
                pv_index = String.valueOf(i);
            if (columns.get(i).equals("click"))
                clk_index = String.valueOf(i);
        }

        if (!column_names.equals("")) {
            column_names = column_names.substring(0, column_names.length()-1);
            column_indices = column_indices.substring(0, column_indices.length()-1);
        } else {
            column_names = "null";
            column_indices = "null";
        }
        System.out.println("column_names: " + column_names);
        System.out.println("column_indices: " + column_indices);
        System.out.println("pv_index: " + pv_index);
        System.out.println("clk_index: " + clk_index);

        for (int i=0; i<args.length; i++) {
            String[] arr = args[i].split(":");
            if (arr.length != 3)
                continue;

            names += arr[0] + ",";
            values += arr[1] + ",";
            weights += arr[2] + ",";
        }
        if (!names.equals("")) {
            names = names.substring(0, names.length()-1);
            values = values.substring(0, values.length()-1);
            weights = weights.substring(0, weights.length()-1);
        } else {
            names = "null";
            values = "null";
            weights = "null";
        }
        System.out.println("names: " + names);
        System.out.println("values: " + values);
        System.out.println("weights: " + weights);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text attr = new Text();
        private Text pv_click = new Text();

        private static String[] column_names;
        private static int[] column_indices;
        private static int pv_index, clk_index;
        private static String[] names;
        private static int[] values;
        private static float[] weights;

        public void setup(Context context) {
            String v = context.getConfiguration().get("column_names");
            String[] column_indices_string;
            String[] values_string;
            String[] weights_string;
            if (!v.equals("null")) {
                column_names = context.getConfiguration().get("column_names").split(",", 0);
                column_indices_string = context.getConfiguration().get("column_indices").split(",", 0);
                names = context.getConfiguration().get("names").split(",", 0);
                values_string = context.getConfiguration().get("values").split(",", 0);
                weights_string = context.getConfiguration().get("weights").split(",", 0);
            } else {
                column_names = new String[0];
                column_indices_string = new String[0];
                names = new String[0];
                values_string = new String[0];
                weights_string = new String[0];
            }
            column_indices = new int[column_indices_string.length];
            for (int i=0; i<column_indices_string.length; i++)
                column_indices[i] = Integer.parseInt(column_indices_string[i]);
            values = new int[values_string.length];
            for (int i=0; i<values_string.length; i++)
                values[i] = Integer.parseInt(values_string[i]);
            weights = new float[weights_string.length];
            for (int i=0; i<weights_string.length; i++)
                weights[i] = Float.parseFloat(weights_string[i]);
            
            pv_index = Integer.parseInt(context.getConfiguration().get("pv_index"));
            clk_index = Integer.parseInt(context.getConfiguration().get("clk_index"));
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] attributes = line.split("\001", -1);

            System.out.println("attributes: " + attributes.length);
            //int start = column_indices[0];

            //构造加入编号的新的记录
            StringBuilder s = new StringBuilder();
            for (int i=0; i<attributes.length; i++) {
                s.append(attributes[i]);
                s.append('\001');
            }

            int match = -1;
            for (int i=0; i<column_names.length; i++) {
                int ind = column_indices[i];
                String name = column_names[i].trim();

				// 匹配条件，假如此纪录满足多个条件的话，以第一条为准
                for (int j=0; j<names.length; j++) {
                    if (names[j].equals(name) && Integer.parseInt(attributes[ind]) == values[j]) {
                        match = j;
                        break;
                    }
                }
                if (match != -1)
                    break;
            }
            
            // 去掉最后一个"\001"
            s.deleteCharAt(s.length()-1);
            attr.set(s.toString());
            pv_click.set("");

            if (match == -1)
                context.write(attr, pv_click);
            else {
                if (Math.random() <= weights[match])
                    context.write(attr, pv_click);
            }
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
		int args_len = otherArgs.length;

        Init(otherArgs[0], otherArgs);

        conf.set("column_names", column_names);
        conf.set("column_indices", column_indices);
        conf.set("pv_index", pv_index);
        conf.set("clk_index", clk_index);
        conf.set("names", names);
        conf.set("values", values);
        conf.set("weights", weights);
		System.out.println("input: " + otherArgs[args_len-2]);
		System.out.println("output: " + otherArgs[args_len-1]);

        Job job = new Job(conf, "sample");
        job.setJarByClass(sample.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置reduce个数为0
        job.setNumReduceTasks(0);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[args_len-2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[args_len-1]));

        job.waitForCompletion(true);
    }
}
