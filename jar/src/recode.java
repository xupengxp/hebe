import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Iterator; 
import java.util.ArrayList;  
import java.io.FileInputStream;
import java.lang.String;
import java.io.*;
import java.util.Random;
import java.lang.Math;
import java.util.HashMap;

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
import hebe.Hebe.Noise;
import hebe.Hebe.Format;
import hebe.Hebe.Filter;
import hebe.Hebe.Settings;
import hebe.Hebe.Settings.Builder;

public class recode {
    public static String last_feature_index = "";
    public static String offset = "";

    public static void Init() throws IOException, DocumentException {
        System.out.println("column_names: " + column_names);
        System.out.println("column_indices: " + column_indices);
        System.out.println("pattern: " + pattern);
        System.out.println("sample_index:" + sample_index);
        System.out.println("random_column:" + random_column);
        System.out.println("random_ratio:" + random_ratio);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text attr = new Text();
        private Text pv_click = new Text();

        private static int last_feature_index, offset;

        public void setup(Context context) {
            String v = context.getConfiguration().get("column_names");
            String[] column_indices_string;
            if (!v.equals("null")) {
                column_names = context.getConfiguration().get("column_names").split(",", 0);
                column_indices_string = context.getConfiguration().get("column_indices").split(",", 0);
            } else {
                column_names = new String[0];
                column_indices_string = new String[0];
            }
            
            column_indices = new int[column_indices_string.length];
            for (int i=0; i<column_indices_string.length; i++)
                column_indices[i] = Integer.parseInt(column_indices_string[i]);

            pattern = context.getConfiguration().get("pattern").trim();

            String sample_index = context.getConfiguration().get("sample_index").trim();
            if (sample_index.equals("-1")) {
                sample_indices = new ArrayList();
                sample_indices.add(0, -1);
            } else {
                String[] sample_index_str = sample_index.split(":", 0);
                sample_indices = new ArrayList();
                for (int i=0; i<sample_index_str.length; i++) 
                    sample_indices.add(i, Integer.parseInt(sample_index_str[i]));
            }
			index = 0;

            String[] random_column_string, random_ratio_string;
            if (!context.getConfiguration().get("random_column").equals("null")) {
                random_column_string = context.getConfiguration().get("random_column").split(",", 0);
                random_ratio_string = context.getConfiguration().get("random_ratio").split(",", 0);
            } else {
                random_column_string = new String[0];
                random_ratio_string = new String[0];
            }
            random_column = new HashMap<String, Float>();
            for (int i=0; i<random_column_string.length; i++)
                random_column.put(random_column_string[i].trim(), Float.parseFloat(random_ratio_string[i]));
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] attributes = line.split(" ", -1);
            //System.out.println("attributes: " + attributes.length);
            String recode_field = attributes[offset];
            String[] recode_field_arr = recode_field.split(",", -1);
            int[] recode_field_over;
            for (int i=0; i<recode_field_arr.length; i++)
                recode_field_over[i] = recode_field_arr + last_feature_index;

            String output;
            for (int i=0; i<attributes.length; i++) {
                if (i==offset) {
                    output += recode_field_over+" ";
                }
                output += attributes[i]+" ";
            }

            String vp2 = vp.replaceAll("-1:1 ", "");
            vp2 = vp2.replaceAll("-1:1", "");

            vp = vp2;
            if (vp.contains("$")) {
                System.out.println("wrong line! (["+vp+"] column_indices.length="+column_indices.length+" pattern="+pattern);
                return;
			}

            int rkey = (int)(Math.random()*100000000);
            attr.set(String.valueOf(rkey));
            pv_click.set(output);

            context.write(attr, pv_click);
            //System.out.println(attr.toString() + "---" + pv_click.toString());
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) {
            try {
                Text v = new Text(); v.set("");
                for (Text value : values) {
                    context.write(value, v);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Init(otherArgs[0], otherArgs[1], otherArgs[2]);

        conf.set("column_names", column_names);
        conf.set("column_indices", column_indices);
        conf.set("pattern", pattern);
        conf.set("sample_index", sample_index);
        conf.set("random_column", random_column);
        conf.set("random_ratio", random_ratio);

        Job job = new Job(conf, "format_output");
        job.setJarByClass(format_output.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));

        job.waitForCompletion(true);
    }
}
