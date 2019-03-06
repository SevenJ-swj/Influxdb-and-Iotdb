package me.testdb.service;

import me.testdb.tool.DBUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InfluxLoadDataService2 {
    InfluxDBService influxDBService=new InfluxDBService();
    DBUtil dbUtil=new DBUtil();
    //log.info(dbName + "    " + table + "    " + tagMap.toString()+ "    " +dataSetEnum+ "    " +filedList+ "    " +readNumber+ "    " +withHeader + "  " + withLabel + "    " +withTimestamp+ "    " +timestampFormatStr);
    //interface(operator, dbName, table, tagsMap, colOrRowEnum, fieldList, limit, withHeader, withTimestamp, timestampFormatStr);

    public void loadData(String path,String dbName,String table,Map<String,List<String>> tagMap,List<String> fieldList,Long readNumber,Boolean withHeader,Boolean withLabel,Boolean withTimestamp,String timestampFormatStr) {
        if (withLabel == true) {
            Integer keyNum = tagMap.keySet().size();
            List<List<String>> arr = new ArrayList<List<String>>();
            List<String> keys = new ArrayList<String>();
            for (String key : tagMap.keySet()) {
                arr.add(tagMap.get(key));
                keys.add(key);
            }
            Integer[] index = new Integer[keyNum];
            Integer[] indexMax = new Integer[keyNum];
            for (int i = 0; i < index.length; i++) {
                index[i] = 0;
                indexMax[i] = arr.get(i).size();
            }
            List<List<String>> res = new ArrayList<List<String>>();
            while (index[0] < indexMax[0]) {
                //for(int i=0;i<keyNum;i++)System.out.print(arr.get(i).get(index[i])+" ");
                //System.out.println();
                StringBuilder sql = new StringBuilder();
                sql.append("select ");
                for (int i = 0; i < fieldList.size(); i++) {
                    if (i == 0) sql.append("\"" + fieldList.get(i) + "\"");
                    else sql.append("," + "\"" + fieldList.get(i) + "\"");
                }

                sql.append(" from " + "\""+table+"\"");

                List<String> ser = new ArrayList<String>();

                for (int i = 0; i < keyNum; i++) {
                    if (i == 0) sql.append(" where ");
                    else sql.append(" and ");
                    String tmpkey="";
                    if(keys.get(i).equals("label")) {tmpkey="class";}
                    else {tmpkey=keys.get(i);}
                    sql.append("\"" + keys.get(i) + "\"" + "=" + "'" + arr.get(i).get(index[i]) + "'");
                    if (keys.get(i).equals("class")) {
                        ser.add(arr.get(i).get(index[i]));
                       // System.out.println("class"+ser.get(0));
                    }
                }
                System.out.println(sql.toString());
                List<List<String>> rs = influxDBService.query(dbName, sql.toString());
                if (rs != null) {
                    //influxDBService.showQueryResult(rs);
                    for (int i = 1; i < rs.size(); i++) {
                        ser.add(rs.get(i).get(1));
                        //System.out.println(rs.get(i).get(1));
                    }
                    res.add(ser);
                }
                int keyId = keyNum - 1;
                index[keyId]++;
                while (index[keyId] >= indexMax[keyId] && keyId >= 1) {
                    index[keyId] = 0;
                    index[keyId - 1]++;
                    keyId--;
                }
            }
            writeCSV(path, res, false, true);
        } else {
            StringBuilder sql = new StringBuilder();
            sql.append("select ");
            for (int i = 0; i < fieldList.size(); i++) {
                if (i == 0) sql.append("\"" + fieldList.get(i) + "\"");
                else sql.append("," + "\"" + fieldList.get(i) + "\"");
            }

            sql.append(" from " + "\""+table+"\"");
            Boolean flag = true;
            if (tagMap != null||tagMap.size()!=0) {
                for (Map.Entry<String, List<String>> entry : tagMap.entrySet()) {
                    if (flag == true) {
                        sql.append(" where ");
                        flag = false;
                    } else sql.append(" and ");

                    for (int i = 0; i < entry.getValue().size(); i++) {
                        if (i != 0) sql.append(" or ");
                        else sql.append("(");
                        sql.append("\"" + entry.getKey() + "\"" + "=" + "'" + entry.getValue().get(i) + "'");
                    }
                    if(entry.getValue()!=null||entry.getValue().size()!=0)sql.append(")");
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
            //influxDBService.showQueryResult(rs);
        }
    }
    public void writeCSV(String path,List<List<String>>Rs,Boolean withHeader,Boolean withTime)
    {
        File file =new File(path);
        FileWriter fw=null;
        BufferedWriter bw=null;
        try {
            fw=new FileWriter(file);
            bw=new BufferedWriter(fw);
            int i=1;
            if(withHeader) i=0;
            for( ;i<Rs.size();i++)
            {
                StringBuilder sb=new StringBuilder();
                String ap="NaN";
                if(Rs.get(i).get(0)!=null||Rs.get(i).get(0).equals("null")) ap=Rs.get(i).get(0);
                if(withTime) sb.append(ap);
                for(int j=1;j<Rs.get(i).size();j++)
                {
                    if(withTime)sb.append(",");
                    else if(j>1) sb.append(",");
                    //System.out.println("BAP:"+Rs.get(i).get(j));
                    if(Rs.get(i).get(j)==null||Rs.get(i).get(j).equals("null")) ap="NaN";
                    else ap=Rs.get(i).get(j);

                    //System.out.println("AP:"+ap);
                    sb.append(ap);
                }
                bw.write(sb.toString()+"\n");
                //System.out.println(sb.toString());
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try{
                bw.close();
                fw.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
