package com.jxlg.briup.bigdata.test;

import jxl.Workbook;
import jxl.read.biff.BiffException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.test
 * @filename: Test.java
 * @create: 2019/11/07 16:29
 * @author: 29314
 * @description: .
 **/

public class Test {
    public static Workbook getXls(String filePath){
        try {
            File file = new File(filePath);
            FileInputStream inputStream = new FileInputStream(file);
            return Workbook.getWorkbook(inputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (BiffException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean isOrgan(String name){
        if(name.matches(".*[大学].*")){
            return true;
        }
        if(name.matches(".*[所].*")){
            return true;
        }
        if(name.matches(".*[公司].*")){
            return true;
        }
        if(name.matches(".*[院].*")){
            return true;
        }
        if(name.matches(".*[社].*")){
            return true;
        }
        if(name.matches(".*[局].*")){
            return true;
        }
        if(name.matches(".*[农场].*")){
            return true;
        }
        if(name.matches(".*[中心].*")){
            return true;
        }
        if(name.matches(".*[厂].*")){
            return true;
        }
        if(name.matches(".*[基地].*")){
            return true;
        }
        if(name.matches(".*[部].*")){
            return true;
        }
        if(name.matches(".*[会].*")){
            return true;
        }
        if(name.matches(".*[机构].*")){
            return true;
        }
        if(name.matches(".*[集团].*")){
            return true;
        }
        if(name.matches(".*[站].*")){
            return true;
        }
        if(name.matches(".*[业].*")){
            return true;
        }
        if(name.matches(".*[室].*")){
            return true;
        }
        if(name.matches(".*[园].*")){
            return true;
        }
        if(name.matches(".*[店].*")){
            return true;
        }
        return false;
    }


    public static void main(String[] args) {
        //System.out.println("2019.8.1".split("[\\.]")[0]);
        //System.out.println("江西理工大学".split(";").length);

        /*Workbook xls = getXls("E:\\学校文件\\稀土专利数据.xls");
        Sheet sheet = xls.getSheet(0);
        for(int i = 1; i< sheet.getRows();i++){
            Cell[] row = sheet.getRow(i);
            String contents = row[4].getContents();
            String[] split = contents.split(";");
            for(String s: split){
                if(!isOrgan(s)){
                    System.out.println(s);
                }
            }
        }*/
        long a = System.currentTimeMillis();
        for(int i = 0;i<10000;i++) {
            System.out.println("abcdefg".contains("cd"));
            System.out.println("abcdefg".contains("dc"));
        }


    }
}
