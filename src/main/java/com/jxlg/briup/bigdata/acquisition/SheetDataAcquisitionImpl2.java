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
 * @description: .对数据表2的采集类
 **/

public class SheetDataAcquisitionImpl2 extends DataAcquisition {
    /*
    获取sheet2的数据
    将 每行的名称 摘要信息发送给服务端
     */
    @Override
    public void setMsg(Socket socket, Workbook workbook) {
        try {
            PrintWriter pw = new PrintWriter(socket.getOutputStream(),true);
            Sheet sheet = workbook.getSheet(1);
            for(int i = 1;i < sheet.getRows();i++){
                Cell[] row = sheet.getRow(i);
                if(row.length==0){
                    break;
                }
                StringBuilder sb = new StringBuilder();
                sb.append(row[0].getContents()).append("\t")
                        .append(row[1].getContents());
                System.out.println(sb.toString());
                pw.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SheetDataAcquisitionImpl2 sdai1 = new SheetDataAcquisitionImpl2();
        Workbook xls = sdai1.getXls("E:\\学校文件\\稀土专利数据.xls");
        Socket socket = sdai1.getSocket("192.168.112.120", 9910);
        sdai1.setMsg(socket,xls);
        System.out.println("发送成功");
    }
}
