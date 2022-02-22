package com.jxlg.briup.bigdata.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.analysis
 * @filename: PatentAttributeAnalysis.java
 * @create: 2019/11/07 18:16
 * @author: 29314
 * @description: .稀土专利属性分析
 **/

public class PatentAttributeAnalysis extends Configured implements Tool {
    public static class PatentAttributeAnalysisMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text k2 = new Text();
        private IntWritable v2 = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String str = split[1];
            if(str.contains("永磁材料")){
                this.k2.set("永磁材料");
                this.v2.set(1);
                context.write(this.k2,this.v2);
            }
            if(str.contains("磁性材料")){
                this.k2.set("磁性材料");
                this.v2.set(1);
                context.write(this.k2,this.v2);
            }
            if(str.contains("合金材料")){
                this.k2.set("合金材料");
                this.v2.set(1);
                context.write(this.k2,this.v2);
            }
            if(str.contains("激光晶体")){
                this.k2.set("激光晶体");
                this.v2.set(1);
                context.write(this.k2,this.v2);
            }
            if(str.contains("发光材料")){
                this.k2.set("发光材料");
                this.v2.set(1);
                context.write(this.k2,this.v2);
            }
        }
    }

    public static class PatentAttributeAnalysisReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text k3 = new Text();
        private IntWritable v3 = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            this.k3.set(key.toString());
            int num = 0;
            for(IntWritable v:values){
                num += v.get();
            }
            this.v3.set(num);
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "稀土专利属性分析");
        job.setJarByClass(this.getClass());

        job.setMapperClass(PatentAttributeAnalysisMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setReducerClass(PatentAttributeAnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PatentAttributeAnalysis(),args));
    }
}
