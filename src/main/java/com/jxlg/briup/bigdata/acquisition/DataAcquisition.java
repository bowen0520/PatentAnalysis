package com.jxlg.briup.bigdata.acquisition;

import jxl.Workbook;
import jxl.read.biff.BiffException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;

/**
 * @program: PatentAnalysis
 * @package: com.jxlg.briup.bigdata.acquisition
 * @filename: DataAcquisition.java
 * @create: 2019/11/06 09:52
 * @author: 29314
 * @description: .从.xls文件中采集有用数据
 **/

public abstract class DataAcquisition {
    /**
     *
     * @param ip：服务ip
     * @param port：服务端口
     * @return：获取远程服务的连接
     */
    public Socket getSocket(String ip,int port){
        try {
            return new Socket(ip,port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param filePath：本地源文件路径
     * @return：获取文件包装流对象
     */
    public Workbook getXls(String filePath){
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

    /**
     * 从包装文件流对象中获取数据通过socket连接发送数据给服务端
     * 发送数据的处理以及格式自定义
     * @param socket
     * @param workbook
     */
    public abstract void setMsg(Socket socket,Workbook workbook);
}
