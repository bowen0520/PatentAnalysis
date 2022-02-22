package com.jxlg.briup.bigdata.dao;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.analysis
 * @filename: YearAreaNum.java
 * @create: 2019/11/07 09:17
 * @author: 29314
 * @description: .自定义数据类型来封装数据
 **/

public class YearAreaData implements WritableComparable<YearAreaData> {
    private Text year;
    private Text area;

    public YearAreaData() {
        this.year = new Text();
        this.area = new Text();
    }

    @Override
    public int compareTo(YearAreaData o) {
        int a = this.year.compareTo(o.year);
        int b = this.area.compareTo(o.area);
        return a==0?b:a;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearAreaData that = (YearAreaData) o;
        return Objects.equals(year, that.year) &&
                Objects.equals(area, that.area);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, area);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.area.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.area.readFields(dataInput);
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

    @Override
    public String toString() {
        return year.toString() + "\t" + area.toString();
    }
}
