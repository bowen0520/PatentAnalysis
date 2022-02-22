package com.jxlg.briup.bigdata.dao;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.analysis
 * @filename: YearAreaNumData.java
 * @create: 2019/11/07 12:53
 * @author: 29314
 * @description: .
 **/

public class YearAreaNumData implements WritableComparable<YearAreaNumData> {
    private Text year;
    private Text area;
    private IntWritable sum;
    private IntWritable num;

    public YearAreaNumData() {
        this.year = new Text();
        this.area = new Text();
        this.sum = new IntWritable();
        this.num = new IntWritable();
    }

    @Override
    public int compareTo(YearAreaNumData o) {
        int a = this.year.compareTo(o.year);
        int b = this.area.compareTo(o.area);
        int c = this.sum.compareTo(o.sum);
        int d = this.num.compareTo(o.num);
        return a==0?(b==0?(c==0?d:c):b):a;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearAreaNumData that = (YearAreaNumData) o;
        return Objects.equals(year, that.year) &&
                Objects.equals(area, that.area) &&
                Objects.equals(sum, that.sum) &&
                Objects.equals(num, that.num);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, area, sum, num);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.area.write(dataOutput);
        this.sum.write(dataOutput);
        this.num.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.area.readFields(dataInput);
        this.sum.readFields(dataInput);
        this.num.readFields(dataInput);
    }

    public String getYear() {
        return year.toString();
    }

    public void setYear(String year) {
        this.year.set(year);
    }

    public String getArea() {
        return area.toString();
    }

    public void setArea(String area) {
        this.area.set(area);
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
        return year.toString() + "\t" + area.toString() + "\t"
                + sum.get() + "\t" + num.get();
    }
}
