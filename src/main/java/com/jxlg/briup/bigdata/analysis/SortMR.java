package com.jxlg.briup.bigdata.analysis;

import com.jxlg.briup.bigdata.dao.YearAreaNumData;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.analysis
 * @filename: SortMR.java
 * @create: 2019/11/07 12:58
 * @author: 29314
 * @description: .
 **/

public class SortMR extends Configured implements Tool {
    public static class SortMRMapper
            extends Mapper<LongWritable, Text, YearAreaNumData, NullWritable> {
        private YearAreaNumData k2 = new YearAreaNumData();
        private NullWritable v2 = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            this.k2.setYear(split[0]);
            this.k2.setArea(split[1]);
            this.k2.setSum(Integer.parseInt(split[2]));
            this.k2.setNum(Integer.parseInt(split[3]));
            context.write(this.k2,this.v2);
        }
    }

    public static class SortMRPartitioner
            extends Partitioner<YearAreaNumData, NullWritable> {
        @Override
        public int getPartition(YearAreaNumData yand,
                                NullWritable nu,
                                int i
        ) {
            return yand.getYear().hashCode()%i;
        }
    }

    public static class SortMRGroupingComparator
            extends WritableComparator {
        public SortMRGroupingComparator() {
            super(YearAreaNumData.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            YearAreaNumData ka = (YearAreaNumData) a;
            YearAreaNumData kb = (YearAreaNumData) b;
            return ka.compareTo(kb);
        }
    }

    public static class SortMRSortComparator
            extends WritableComparator {
        public SortMRSortComparator() {
            super(YearAreaNumData.class,true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            YearAreaNumData ka = (YearAreaNumData) a;
            YearAreaNumData kb = (YearAreaNumData) b;
            int x = kb.getSum()-ka.getSum();
            int y = kb.getNum()-ka.getNum();
            return x==0?y:x;
        }
    }

    public static class SortMRReducer
            extends Reducer<YearAreaNumData, NullWritable, Text, NullWritable> {
        private Text k3 = new Text();
        private NullWritable v3 = NullWritable.get();
        @Override
        protected void reduce(YearAreaNumData key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            k3.set(key.toString());
            context.write(this.k3,this.v3);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Path in = new Path(conf.get("in"));
        Path out = new Path(conf.get("out"));

        Job job = Job.getInstance(conf, "专利授权统计排序");
        job.setJarByClass(this.getClass());

        job.setMapperClass(SortMRMapper.class);
        job.setMapOutputKeyClass(YearAreaNumData.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,in);

        job.setPartitionerClass(SortMRPartitioner.class);
        job.setGroupingComparatorClass(SortMRGroupingComparator.class);
        job.setSortComparatorClass(SortMRSortComparator.class);

        job.setReducerClass(SortMRReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job,out);

        job.setNumReduceTasks(6);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SortMR(),args));
    }
}
