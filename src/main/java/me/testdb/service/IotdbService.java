package me.testdb.service;

import com.mongodb.client.MongoCollection;
import me.testdb.tool.DBUtil;
import me.testdb.tool.Pair;
import me.testdb.tool.Structure;
import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


import java.io.*;
import java.sql.*;
import java.util.*;

public class IotdbService {

    private List<String> prefixSeriesList=new ArrayList<String>();
    private Integer batchNum = 10000;
    private  String defaultDbName = "test";
    private DBUtil dbUtil = new DBUtil();
    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://10.131.247.51:6667/", "root", "root");
            statement = connection.createStatement();
            boolean hasResultSet = statement.execute("select c from root.a.b");
            //statement.execute("insert into root.vehicle.d0(timestamp,s0) values(1,101)");
            if (hasResultSet) {
                ResultSet res = statement.getResultSet();
                System.out.println(res.getMetaData().getColumnCount());
                for(int i=1;i<=res.getMetaData().getColumnCount();i++)
                    System.out.println(res.getMetaData().getColumnName(i));
                while (res.next()) {
                    System.out.println(res.getString("Time") + " | " + res.getString("root.a.b.c"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    public void getSeries(JSONObject jsonObject,String preSeriesName) {
        for(Object key:jsonObject.keySet())
        {
            try {
                JSONObject nextJsonObject=jsonObject.getJSONObject(key.toString());
                getSeries(nextJsonObject,preSeriesName+"."+key.toString());
            }
            catch (Exception e){
                prefixSeriesList.add(preSeriesName+"."+jsonObject.getString(key.toString()));
            }
        }
    }

    public void updateMetaData(String dbName,String table,JSONObject tagMap,List<String> fieldNameList,List<String> fieldTypeList)
    {
        JSONObject metadata=new JSONObject();
        metadata.put("id","-1");
        metadata.put("token","admin");
        metadata.put("auth",2);
        metadata.put("dataSet",dbName);
        metadata.put("subDataSet",table);
        metadata.put("description","");
        metadata.put("type",1);
        metadata.put("source",new JSONArray());
        metadata.put("target","");
        JSONArray fieldName=JSONArray.fromObject(fieldNameList);
        metadata.put("fieldName",fieldName);
        JSONArray fieldType=JSONArray.fromObject(fieldTypeList);
        metadata.put("fieldType",fieldType);
        metadata.put("columns",-1);
        metadata.put("rows",new JSONArray());
        metadata.put("dbType",2);
        metadata.put("dbName",dbName);
        metadata.put("table",table);
        metadata.put("tag",tagMap);
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        metaDataService.saveMetaData(metadata.toString(),mongoCollection);
        metaDataService.close();
    }

    public Boolean batchInsert(String path, String measurement, String dbName, Structure structure) throws SQLException {
        System.out.println("batchInsert");
        Boolean status=true;
        File file = null;
        try {
            file = new File(path);  // file path
            String[] fileinfo = file.getName().split("\\.");
            System.out.println("file format:" + fileinfo[fileinfo.length - 1]);
        } catch (Exception e) {
            System.out.println("open file failed,please check path\n" + e);
            return false;
        }
        if (dbName == null || dbName.equals(""))
            dbName = this.defaultDbName; //if dbName is null set dbName to default:"test"
        String storageGroup = "root." + dbName + "." + measurement;
        BufferedReader br = null;
        Long excuteTime = 0L;

        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://10.131.247.51:6667/", "root", "root");
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            try {
                statement.execute("SET STORAGE GROUP TO " + storageGroup);
            }
            catch (Exception e) {
                System.out.println(e);
            }
            List<Pair<String, String>> header = structure.getFileStruct();
            for (int i = 0; i < header.size(); i++) {
                if (header.get(i).getFirst().toLowerCase().equals("time") || header.get(i).getFirst().toLowerCase().equals("tag"))
                    continue;
                String seriesName = storageGroup + "." + header.get(i).getFirst();
                String dataType = header.get(i).getSecond();
                String encoding = "RLE";
                if (dataType.equals("Long")) dataType = "INT64";
                if (dataType.equals("String")) {
                    dataType = "TEXT";
                    encoding = "PLAIN";
                }
                dataType.toUpperCase();
                try{
                    statement.execute("CREATE TIMESERIES " + seriesName + " WITH DATATYPE=" + dataType + ", ENCODING=" + encoding);
                }catch (Exception e){
                    System.out.println(e);
                }
            }

            String timeFormat = structure.getTimeFormat();
            String line = "";
            Long sysTime = 0L;
            Long batchCnt = 0L;
            br = new BufferedReader(new FileReader(file));
            Integer startLine = structure.getStartLine();
            while (startLine > 1) {
                startLine--;
                br.readLine();
            }
            while ((line = br.readLine()) != null && line != "") {
                if (sysTime % 1000000 == 0) System.out.println(sysTime);
                line = line.replace("\"", "");
                String[] objList = line.split(",", -1);
                try {
                    String time = "";
                    time = String.valueOf(++sysTime);
                    for (int i = 0; i < header.size(); i++) {
                        String K = header.get(i).getFirst();
                        String V = header.get(i).getSecond();
                        if (V.equals("Time")) {
                            if (structure.getTimeFormat() == null || structure.getTimeFormat().equals(""))
                                time = objList[i];
                            else time = dbUtil.parseTime(objList[i], timeFormat);
                            break;
                        }
                    }
                    for (int i = 0; i < objList.length; i++) {
                        if (objList[i].equals("") || objList[i].equals("NaN") || objList[i] == null) continue;
                        String colName = header.get(i).getFirst();
                        String seriesName = storageGroup ;
                        String insert_sql = "insert into " + seriesName + "(timestamp," + colName + ") values(" + time + "," + objList[i] + ");";
                        //System.out.println(insert_sql);
                        statement.addBatch(insert_sql);
                        batchCnt++;
                        if (batchCnt % batchNum == 0) {
                            statement.executeBatch();
                            //connection.commit();
                        }
                    }
                }
                catch (Exception e){
                    System.out.println(e);
                }
            }
            statement.executeBatch();
            //connection.commit();

            List<String> fieldNameList=new ArrayList<>();
            List<String> fieldTypeList=new ArrayList<>();
            for(int i=0;i<structure.getFileStruct().size();i++)
            {
                String _name=structure.getFileStruct().get(i).getFirst();
                String _type=structure.getFileStruct().get(i).getSecond();
                fieldNameList.add(_name);
                fieldTypeList.add(_type);
            }
            updateMetaData(dbName,measurement,new JSONObject(),fieldNameList,fieldTypeList);
        }
        catch (Exception e) {
            e.printStackTrace();
            status = false;
        }
        finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
            return status;
        }
    }


    public Boolean batchInsert(String path, String measurement, String dbName, Boolean  horizon) throws SQLException {
        if(horizon==false) return batchInsert(path,measurement,dbName);
        else {
            String trainTest="";
            System.out.println("batchInsert");
            Boolean status=true;
            File file = null;
            try {
                file = new File(path);  // file path
                String[] fileinfo = file.getName().split("\\.");
                System.out.println("file format:" + fileinfo[fileinfo.length - 1]);
                trainTest = fileinfo[0].split("_")[fileinfo[0].split("_").length-1];
            } catch (Exception e) {
                System.out.println("open file failed,please check path\n" + e);
                return false;
            }
            if (dbName == null || dbName.equals(""))
                dbName = this.defaultDbName; //if dbName is null set dbName to default:"test"
            String storageGroup = "root." + dbName + "." + measurement;
            BufferedReader br = null;
            Long excuteTime = 0L;
            JSONObject tagMap = new JSONObject();
            Connection connection = null;
            Statement statement = null;
            try {
                Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
                connection = DriverManager.getConnection("jdbc:tsfile://10.131.247.51:6667/", "root", "root");
                connection.setAutoCommit(false);
                statement = connection.createStatement();
                try {
                    statement.execute("SET STORAGE GROUP TO " + storageGroup);
                }
                catch (Exception e) {
                    System.out.println(e);
                }
                String line = "";
                Long sysTime = 0L;
                Long batchCnt = 0L;
                Long seriesNum = 0L;
                br = new BufferedReader(new FileReader(file));
                Map<String,JSONObject> sereisJson=new TreeMap<>();
                while ((line = br.readLine()) != null && line != "") {
                    seriesNum++;
                    String[] obj=line.split(",");
                    String dataType = "DOUBLE";
                    String encoding = "RLE";
                    String seriesName = storageGroup+"."+trainTest+".class_"+obj[0]+".series"+seriesNum;
                    String prefixClass="class_";
                    if(!sereisJson.containsKey(prefixClass+obj[0]))
                        sereisJson.put(prefixClass+obj[0],new JSONObject());
                    JSONObject seriesOBJ=sereisJson.get(prefixClass+obj[0]);
                    try{
                        seriesOBJ.put("series"+seriesNum,"series"+seriesNum);
                        sereisJson.put(prefixClass+obj[0],seriesOBJ);
                        statement.execute("CREATE TIMESERIES " + seriesName +".val" + " WITH DATATYPE=" + dataType + ", ENCODING=" + encoding);
                    }catch (Exception e){
                        System.out.println(e);
                    }

//                    PreparedStatement insert_sql=connection.prepareStatement("insert into "+seriesName+"(timestamp,val) values(?,?)");

                    for(int i=1;i<obj.length;i++)
                    {
                        //System.out.println(obj[i]);
                        statement.addBatch("insert into "+seriesName+"(timestamp,val) values("+i+","+obj[i]+")");
                       // insert_sql.setString(1,String.valueOf(i));
                       // insert_sql.setDouble(2,Double.valueOf(obj[i]));
                        batchCnt++;
                        if(batchCnt%batchNum==0) statement.executeBatch();
                    }
                }
                statement.executeBatch();
                JSONObject typeOBJ = JSONObject.fromObject(sereisJson);
              /*  JSONObject typeOBJ=new JSONObject();
                for(String key:sereisJson.keySet())
                {
                    JSONObject jsonObject=sereisJson.get(key);
                    typeOBJ.put(key,jsonObject);
                }*/
               JSONObject classOBJ=new JSONObject();
               classOBJ.put(trainTest,typeOBJ);
               tagMap.put("tag",classOBJ);
               // System.out.println(tagMap.toString());

                //connection.commit();

                List<String> fieldNameList=new ArrayList<>();
                List<String> fieldTypeList=new ArrayList<>();
                fieldNameList.add("val");
                fieldTypeList.add("Double");
                updateMetaData(dbName,measurement,tagMap,fieldNameList,fieldTypeList);
            }
            catch (Exception e) {
                e.printStackTrace();
                status = false;
            }
            finally {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
                return status;
            }
        }
    }


    public Boolean batchInsert(String path, String measurement, String dbName) throws SQLException {
        Structure structure=dbUtil.parseFile(path);
        return  batchInsert(path,measurement,dbName,structure);
    }

    public List<List<String>> query(String preSeriesName,List<String> fieldList,Long readNum,String timeFormat) throws SQLException
    {
        Connection connection = null;
        Statement statement = null;
        List<List<String>> rs=new ArrayList<List<String>>();
        try {
            Class.forName("cn.edu.tsinghua.iotdb.jdbc.TsfileDriver");
            connection = DriverManager.getConnection("jdbc:tsfile://10.131.247.51:6667/", "root", "root");
            statement = connection.createStatement();
            String selectEntity="";
            for(int i=0;i<fieldList.size();i++)
            {
                if(i==0) selectEntity=selectEntity+fieldList.get(i);
                else selectEntity=selectEntity+","+fieldList.get(i);
            }
            if(fieldList.size()==0) selectEntity="*";
            String querySql="select "+selectEntity+" from "+ preSeriesName;
            if(readNum!=0) querySql=querySql +" limit "+readNum;
            //System.out.println(querySql);
            boolean hasResultSet = statement.execute(querySql);

            if (hasResultSet) {
                List<String> head=new ArrayList<String>();
                ResultSet res = statement.getResultSet();
                for(int i=1;i<=res.getMetaData().getColumnCount();i++)
                {
                    head.add(res.getMetaData().getColumnName(i));
                }
                rs.add(head);
                while (res.next()) {
                    List<String> line=new ArrayList<String>();
                    for(int i=1;i<=res.getMetaData().getColumnCount();i++)
                    {
                        if(i==1&&!timeFormat.equals(""))
                            line.add(dbUtil.stampToDefDate(Long.valueOf(res.getString("Time")),timeFormat));
                        else if(i==1) line.add(res.getString("Time"));
                        else if(i!=1) line.add(String.valueOf(res.getString(preSeriesName+"."+fieldList.get(i-2))));
                    }
                    rs.add(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return rs;
    }

    public void loadData(
            String path,
            String dbName,
            String table,
            JSONObject tagMap,
            List<String> fieldList,
            Long readNumber,
            Boolean withHeader,
            Boolean withLabel,
            Boolean withTimestamp,
            String timestampFormatStr) throws SQLException{
        prefixSeriesList.clear();
        String storageGroup = "root."+dbName+"."+table;
        if(tagMap.getJSONObject("tag").size()!=0) getSeries(tagMap.getJSONObject("tag"),storageGroup);
        else prefixSeriesList.add(storageGroup);
        List<List<String>> rs = new ArrayList<List<String>>();
        for(int i=0;i<prefixSeriesList.size();i++)
        {
            //System.out.println(prefixSeriesList.get(i));
            rs = this.query(prefixSeriesList.get(i),fieldList,readNumber,timestampFormatStr);
            //for(int j=0;j<rs.size();j++) System.out.println(rs.get(j).get(0)+"|"+rs.get(j).get(1));
            if(withLabel==true)
            {
                withHeader=true;
                withTimestamp=true;
                List<List<String>> transRs=new ArrayList<List<String>>();
                List<String> line=new ArrayList<String>();
                String[] objList=rs.get(0).get(1).split("\\.");
                //System.out.println(objList[objList.length-3]);
                line.add(objList[objList.length-3].split("_")[1]);
                for(int j=1;j<rs.size();j++){
                    line.add(rs.get(j).get(1));
                }
                transRs.add(line);
                rs=transRs;
            }
            try{
                Boolean append = (i==0?false:true);
                /*System.out.print("line"+i+":");
                for(int j=0;j<rs.get(0).size();j++)
                {
                    System.out.print(rs.get(0).get(j)+" ");
                }
                System.out.println();*/
                writeCSV(path,rs,withHeader,withTimestamp,append);
            }catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    private void writeCSV(String path, List<List<String>> Rs, Boolean withHeader, Boolean withTime,Boolean append) throws IOException{
        File file = new File(path);
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(file,append);
            bw = new BufferedWriter(fw);
            int i = 1;
            if (withHeader) i = 0;
            for (; i < Rs.size(); i++) {
                StringBuilder sb = new StringBuilder();
                String val = "NaN";
                if (Rs.get(i).get(0) != null || !Rs.get(i).get(0).equals("null")) val = Rs.get(i).get(0);
                if (withTime) sb.append(val);
                for (int j = 1; j < Rs.get(i).size(); j++) {
                    if (withTime) sb.append(",");
                    else if (j > 1) sb.append(",");
                    //System.out.println("BAP:"+Rs.get(i).get(j));
                    if (Rs.get(i).get(j) == null || Rs.get(i).get(j).equals("null")) val = "NaN";
                    else val = Rs.get(i).get(j);
                    sb.append(val);
                }
                bw.write(sb.toString() + "\n");
                //System.out.println(sb.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            bw.close();
            fw.close();
        }
    }
}
