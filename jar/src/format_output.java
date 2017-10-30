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

public class format_output {

    public static String pattern = "";
    public static String column_names = "";
    public static String column_indices = "";
    public static String sample_index = "";
    public static String random_ratio = "1", random_column = "null";

    public static void Init(String path, String name, String _sample_index) throws IOException, DocumentException {
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
                if (columns.indexOf(part_columns[k].trim()) == -1)
                    columns.add(part_columns[k].trim());
            }
        }
        List<Noise> noise = settings.getNoiseList();
        for (int i=0; i<noise.size(); i++) {
            Noise item = noise.get(i);
            String[] to_columns = item.getTo().split(",");
            for (int k=0; k<to_columns.length; k++) {
                if (columns.indexOf(to_columns[k]) == -1)
                    columns.add(to_columns[k].trim());
            }
        }
        List<Transform> transform = settings.getTransformList();
        for (int i=0; i<transform.size(); i++) {
            Transform trans = transform.get(i);
            String[] trans_columns = trans.getColumns().split(",", 0);
			String[] trans_to = trans.getTo().split(",");
            for (int k=0; k<trans_columns.length; k++) {
                int ind = columns.indexOf(trans_columns[k].trim());
                if (ind != -1) {
                    columns.add(trans_to[k].trim());
				}
			}
        }
		List<Filter> filters = settings.getFilterList();
		for (int i=0; i<filters.size(); i++) {
			Filter flt = filters.get(i);
			String flt_outputs = flt.getOutputs();
			if (flt_outputs.indexOf("%d") == -1) {
				String[] outputs = flt_outputs.split(",");
				for (int k=0; k<outputs.length; k++)
					columns.add(outputs[k].trim());
			} else {
				int output_num = flt.getOutputNum();
				for (int k=0; k<output_num; k++) {
					columns.add(flt_outputs.replace("%d", String.valueOf(k)));
				}
			}
		}

        List<Format> format = settings.getFormatList();
		String fmt_columns = "";
        for (int i=0; i<format.size(); i++) {
            Format fmt = format.get(i);
            if (!fmt.getName().equals(name))
                continue;

            fmt_columns = fmt.getColumns().trim();
            pattern = fmt.getPattern().trim();
    
            if (fmt.hasRandomColumn()) {
                random_column = fmt.getRandomColumn().trim();
                random_ratio = fmt.getRandomRatio().trim();
            } 
        }
		String[] fmts = fmt_columns.split(",");
        for (int i=0; i<fmts.length; i++) {
        	//System.out.println(fmts[i]);
			int ind = columns.indexOf(fmts[i].trim());
            if (ind != -1) {
                column_names += fmts[i].trim() + ",";
                column_indices += String.valueOf(ind) + ",";
        	}
		}

        if (!column_names.equals("")) {
            column_names = column_names.substring(0, column_names.length()-1);
            column_indices = column_indices.substring(0, column_indices.length()-1);
        } else {
            column_names = "null";
            column_indices = "null";
        }
		sample_index = _sample_index;

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

        private static String[] column_names;
        private static int[] column_indices;
        private static String pattern;
        private static ArrayList sample_indices;
        private static int index;
        private static HashMap<String, Float> random_column;

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
            String[] attributes = line.split("\001", -1);
            //System.out.println("attributes: " + attributes.length);
			int max_ind = column_indices[column_indices.length-1];
			for (int i=0; i<column_indices.length; i++)
				if (max_ind < column_indices[i])
					max_ind = column_indices[i];
            if (column_indices.length > 0 && attributes.length <= max_ind) {
                System.out.println("wrong line! (["+line+"] attributes.length="+attributes.length);
                return;
            }

            String vp = pattern;
            int aaa=0;
            for (int cursor=1; cursor<=column_indices.length; cursor++) {
                String dim = "${" + cursor + "}";
                String _value = attributes[column_indices[cursor-1]].trim();
                String _name = column_names[cursor-1];

                if (random_column.containsKey(_name)) {
                    float rand_thresh = random_column.get(_name);
                    if (Math.random() < rand_thresh) {
                        vp = vp.replace(dim, "-1"); aaa = 1;
                    } else 
                        vp = vp.replace(dim, _value);
                } else
                    vp = vp.replace(dim, _value);
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
            pv_click.set(vp);

			if (sample_indices.contains(index) || sample_indices.contains(-1))
                context.write(attr, pv_click);

            index += 1;
            if (index == 10)
                index = 0;
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
