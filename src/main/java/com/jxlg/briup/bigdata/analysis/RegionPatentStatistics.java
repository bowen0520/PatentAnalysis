package com.jxlg.briup.bigdata.analysis;

import com.jxlg.briup.bigdata.dao.NumsData;
import com.jxlg.briup.bigdata.dao.YearAreaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.analysis
 * @filename: RegionPatentStatistics.java
 * @create: 2019/11/07 09:29
 * @author: 29314
 * @description: .地区授权专利统计
 * 名称   年份  地区  机构  类型
 **/

public class RegionPatentStatistics extends Configured implements Tool {
    public static class RegionPatentStatisticsMapper
            extends Mapper<LongWritable, Text, YearAreaData, NumsData> {
        private YearAreaData k2 = new YearAreaData();
        private NumsData v2 = new NumsData();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");

            String year = split[1].split("\\.")[0];
            String region = split[2];
            this.k2.setYear(year);
            this.k2.setArea(region);

            this.v2.setSum(1);
            int num = split[4].startsWith("发明")?1:0;
            this.v2.setNum(num);
            System.out.println(k2.toString()+"\t"+v2.toString());
            context.write(this.k2,this.v2);
        }
    }

    public static class RegionPatentStatisticsPartitioner
            extends Partitioner<YearAreaData, NumsData> {
        @Override
        public int getPartition(YearAreaData yad,
                                NumsData nd,
                                int i
        ) {
            return yad.getYear().hashCode()%i;
        }
    }

    public static class RegionPatentStatisticsGroupingComparator
            extends WritableComparator {
        public RegionPatentStatisticsGroupingComparator() {
            super(YearAreaData.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            YearAreaData ka = (YearAreaData) a;
            YearAreaData kb = (YearAreaData) b;
            return ka.getArea().compareTo(kb.getArea());
        }
    }

    public static class RegionPatentStatisticsReducer
            extends Reducer<YearAreaData, NumsData, Text, NullWritable> {
        private Text k3 = new Text();
        private NullWritable v3 = NullWritable.get();
        @Override
        protected void reduce(YearAreaData key, Iterable<NumsData> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            sb.append(key.getYear()).append("\t")
                    .append(key.getArea()).append("\t");
            int sum = 0;
            int num = 0;
            for(NumsData v:values){
                sum += v.getSum();
                num += v.getNum();
            }
            sb.append(sum).append("\t").append(num);
            k3.set(sb.toString());
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "区域专利授权统计");
        job.setJarByClass(this.getClass());

        job.setMapperClass(RegionPatentStatisticsMapper.class);
        job.setMapOutputKeyClass(YearAreaData.class);
        job.setMapOutputValueClass(NumsData.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setPartitionerClass(RegionPatentStatisticsPartitioner.class);
        job.setGroupingComparatorClass(RegionPatentStatisticsGroupingComparator.class);
        job.setSortComparatorClass(RegionPatentStatisticsGroupingComparator.class);

        job.setReducerClass(RegionPatentStatisticsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,out);

        job.setNumReduceTasks(6);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new RegionPatentStatistics(),args));
    }
}
