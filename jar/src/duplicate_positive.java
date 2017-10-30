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

public class duplicate_positive {
    public static void Init() throws IOException, DocumentException {
        //System.out.println("random_ratio:" + random_ratio);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text attr = new Text();
        private Text pv_click = new Text();

        public void setup(Context context) {
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] attributes = line.split(" ", -1);

            String line2 = "";
            if (attributes[0] == "1")
                line2 += "0 ";
            for (int i=1; i<attributes.length; i++) {
                line2 += attributes[i];
                if (i != (attributes.length-1))
                    line2 += " ";
            }
            int rkey = (int)(Math.random()*100000000);
            attr.set(String.valueOf(rkey));
            pv_click.set(line);
            context.write(attr, pv_click);

            if (attributes[0] == "1") {
            rkey = (int)(Math.random()*100000000);
            attr.set(String.valueOf(rkey));
            pv_click.set(line2);
            context.write(attr, pv_click); }
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
        Init();

        Job job = new Job(conf, "duplicate_positive");
        job.setJarByClass(format_output.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.out.println("input:" + otherArgs[0] + ", output:" + otherArgs[1]);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
    }
}
