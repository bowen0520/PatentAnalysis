package com.jxlg.briup.bigdata.dao;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.analysis
 * @filename: NumsData.java
 * @create: 2019/11/07 15:52
 * @author: 29314
 * @description: .
 **/

public class NumsData implements WritableComparable <NumsData> {
    private IntWritable sum;
    private IntWritable num;

    public NumsData() {
        this.sum = new IntWritable();
        this.num = new IntWritable();
    }

    @Override
    public int compareTo(NumsData o) {
        int a = this.sum.compareTo(o.sum);
        int b = this.num.compareTo(o.num);

        return a==0?b:a;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NumsData numsData = (NumsData) o;
        return Objects.equals(sum, numsData.sum) &&
                Objects.equals(num, numsData.num);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, num);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.sum.write(dataOutput);
        this.num.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum.readFields(dataInput);
        this.num.readFields(dataInput);
    }

    public int getSum() {
        return sum.get();
    }

    public void setSum(int sum) {
        this.sum.set(sum);
    }

    public int getNum() {
        return num.get();
    }

    public void setNum(int num) {
        this.num.set(num);
    }

    @Override
    public String toString() {
        return sum.get() + "\t" + num.get() ;
    }
}
