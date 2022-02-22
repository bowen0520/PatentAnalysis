package com.jxlg.briup.bigdata.acquisition;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.acquisition
 * @filename: SheetDataAcquisitionImpl1.java
 * @create: 2019/11/06 13:05
 * @author: 29314
 * @description: .对数据表1的采集类
 **/

public class SheetDataAcquisitionImpl1 extends DataAcquisition {
    @Override
    /*
    获取sheet1的数据
    将最终法律状态不是授权的和地区不是国内的行信息全部清除
    获取剩下的行信息中的 名称   申请日 国省代码    申请人 专利类型列信息发送给服务端
     */
    public void setMsg(Socket socket, Workbook workbook) {
        try {
            PrintWriter pw = new PrintWriter(socket.getOutputStream(),true);
            Sheet sheet = workbook.getSheet(0);
            for(int i = 1;i < sheet.getRows();i++){
                Cell[] row = sheet.getRow(i);
                if(row.length==0){
                    break;
                }
                if("授权".equals(row[2].getContents())){
                    String[] split = row[3].getContents().split(";");
                    if (split.length==2&&split[1].matches("[\\d]{2}")){
                        StringBuilder sb = new StringBuilder();
                        sb.append(row[0].getContents()).append("\t")
                                .append(row[1].getContents()).append("\t")
                                .append(row[3].getContents()).append("\t")
                                .append(row[4].getContents()).append("\t")
                                .append(row[8].getContents());
                        System.out.println(sb.toString());
                        pw.println(sb.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SheetDataAcquisitionImpl1 sdai1 = new SheetDataAcquisitionImpl1();
        Workbook xls = sdai1.getXls("E:\\学校文件\\稀土专利数据.xls");
        Socket socket = sdai1.getSocket("192.168.112.120", 9909);
        sdai1.setMsg(socket,xls);
        System.out.println("发送成功");
    }
}
