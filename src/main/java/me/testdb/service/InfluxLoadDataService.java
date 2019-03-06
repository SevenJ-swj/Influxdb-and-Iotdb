package me.testdb.service;

import com.sun.org.apache.xpath.internal.operations.Bool;
import me.testdb.tool.DBUtil;
import me.testdb.tool.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InfluxLoadDataService {
    InfluxDBService influxDBService = new InfluxDBService();

    DBUtil dbUtil=new DBUtil();
    //log.info(dbName + "    " + table + "    " + tagMap.toString()+ "    " +dataSetEnum+ "    " +filedList+ "    " +readNumber+ "    " +withHeader + "  " + withLabel + "    " +withTimestamp+ "    " +timestampFormatStr);
    //interface(operator, dbName, table, tagsMap, colOrRowEnum, fieldList, limit, withHeader, withTimestamp, timestampFormatStr);

    public void loadData(String path, String dbName, String table, Map<String, List<String>> tagMap, List<String> fieldList, Long readNumber, Boolean withHeader, Boolean withLabel, Boolean withTimestamp, String timestampFormatStr) {

        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        int f=0;
        if(tagMap.size()!=0||tagMap!=null) {
            for (String key : tagMap.keySet()) {
                if (f == 0) {sql.append("\"" +key + "\"");f=1;}
                else sql.append("," + "\"" + key + "\"");
            }
        }
        for (int i = 0; i < fieldList.size(); i++) {
            if (f == 0) {sql.append("\"" + fieldList.get(i) + "\"");f=1;}
            else sql.append("," + "\"" + fieldList.get(i) + "\"");
        }

        sql.append(" from " + "\"" + table + "\"");
        Boolean flag = true;
        if (tagMap != null || tagMap.size() != 0) {
            for (Map.Entry<String, List<String>> entry : tagMap.entrySet()) {
                if (entry.getValue().size() == 0) continue;
                if (flag == true) {
                    sql.append(" where ");
                    flag = false;
                } else sql.append(" and ");

                int len = entry.getValue().size();
                for (int i = 0; i < len; i++) {
                    if (i != 0) sql.append(" or ");
                    else sql.append("(");
                    sql.append("\"" + entry.getKey() + "\"" + "=" + "'" + entry.getValue().get(i) + "'");
                    if (i == len - 1) {
                        sql.append(")");
                    }
                }
            }
        }
        if (readNumber != 0) {
            sql.append(" limit " + readNumber.toString());
        }
        System.out.println(sql.toString());
        List<List<String>> rs = influxDBService.query(dbName, sql.toString());

        if (withTimestamp) {
            Long ts = 0L;
            for (int i = 1; i < rs.size(); i++, ts += 60 * 1000L) {
                rs.get(i).set(0, dbUtil.stampToDefDate(Long.valueOf(ts), timestampFormatStr));
            }
        }
        if (rs != null)
            writeCSV(path, rs, withHeader, withTimestamp);
        //System.out.println(rs.size());
        //this.showQueryResult(rs);
    }

    public void writeCSV(String path, List<List<String>> Rs, Boolean withHeader, Boolean withTime) {
        File file = new File(path);
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(file);
            bw = new BufferedWriter(fw);
            int i = 1;
            if (withHeader) i = 0;
            for (; i < Rs.size(); i++) {
                StringBuilder sb = new StringBuilder();
                String ap = "NaN";
                if (Rs.get(i).get(0) != null || Rs.get(i).get(0).equals("null")) ap = Rs.get(i).get(0);
                if (withTime) sb.append(ap);
                for (int j = 1; j < Rs.get(i).size(); j++) {
                    if (withTime) sb.append(",");
                    else if (j > 1) sb.append(",");
                    //System.out.println("BAP:"+Rs.get(i).get(j));
                    if (Rs.get(i).get(j) == null || Rs.get(i).get(j).equals("null")) ap = "NaN";
                    else ap = Rs.get(i).get(j);

                    //System.out.println("AP:"+ap);
                    sb.append(ap);
                }
                bw.write(sb.toString() + "\n");
                //System.out.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bw.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
