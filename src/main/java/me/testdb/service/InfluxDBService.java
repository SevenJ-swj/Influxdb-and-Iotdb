package me.testdb.service;

import com.mongodb.client.MongoCollection;
import me.testdb.tool.DBUtil;
import me.testdb.tool.InfluxConfig;
import me.testdb.tool.Pair;
import me.testdb.tool.Structure;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BoundParameterQuery;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class InfluxDBService {

    private InfluxConfig influxConfig=new InfluxConfig();

    private DBUtil dbUtil=new DBUtil();
    private InfluxDBUtil conPoolUtil = new InfluxDBUtil();

    private Integer batchNum = 1000; //default batch Point Number
    private Integer batchTime = 5000; //default batch milliSecond
    private String defaultDbName = "test";

    public void setBatchNum(Integer batchNum) {
        this.batchNum = batchNum;
    }

    public void setBatchTime(Integer batchTime) {
        this.batchTime = batchTime;
    }

    public InfluxDB getConnection() {

        InfluxDB influxDB=null;
        try {
            influxDB = conPoolUtil.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return influxDB;
    }

    public void closeConnection(InfluxDB influxDB) {
        //System.out.println(influxDB.toString()+" Closing");
        if (influxDB != null)
            try{
                conPoolUtil.CloseConnection(influxDB);
            }
            catch (Exception e){
                System.out.println(e);
            }
    }

    /*
     * An Influx Point contains measurementName,time,fields[,tags];
     */
    public Point generatePoint(String measurement, String time,
                               Map<String, Long> fieldLong,
                               Map<String, Double> fieldDouble,
                               Map<String, String> fieldString,
                               Map<String, Boolean> fieldBoolean,
                               Map<String, String> tag) {

        Point.Builder builder = Point.measurement(measurement);
        //System.out.println(time);
        builder.time(Long.valueOf(time), TimeUnit.MILLISECONDS);
        try {
            if (fieldLong!=null&&fieldLong.size() != 0)
                for (Map.Entry<String, Long> entry : fieldLong.entrySet()) {
                    builder.addField(entry.getKey(), entry.getValue());
                }
            if (fieldDouble!=null&&fieldDouble.size() != 0)
                for (Map.Entry<String, Double> entry : fieldDouble.entrySet()) {
                    builder.addField(entry.getKey(), entry.getValue());
                }
            if (fieldBoolean!=null&&fieldBoolean.size() != 0)
                for (Map.Entry<String, Boolean> entry : fieldBoolean.entrySet()) {
                    builder.addField(entry.getKey(), entry.getValue());
                }
            if (fieldString!=null&&fieldString.size() != 0)
                for (Map.Entry<String, String> entry : fieldString.entrySet()) {
                    builder.addField(entry.getKey(), entry.getValue());
                }
            if (tag!=null&&tag.size() != 0)
                for (Map.Entry<String, String> entry : tag.entrySet()) {
                    builder.tag(entry.getKey(), entry.getValue());
                }
        } catch (Exception e) {
            System.out.println("build point failed");
            return null;
        }
        return builder.build();
    }

    public void insertPoint(String dbName, Point point) {
        InfluxDB influxDB = this.getConnection();
        influxDB.setDatabase(dbName);
        influxDB.write(point);
        this.closeConnection(influxDB);
    }

    public void batchInserPoint(String dbName, List<Point> point) {
        InfluxDB influxDB = this.getConnection();
        influxDB.enableBatch(this.batchNum, this.batchTime, TimeUnit.MILLISECONDS);
        influxDB.setDatabase(dbName);
        for (Point p : point) {
            influxDB.write(p);
        }
        this.closeConnection(influxDB);
    }


    public void insertPoint(Point point,InfluxDB influxDB) {
        influxDB.write(point);
        //this.closeConnection(influxDB);
    }


    public void updateMetaData(String dbName, String table, List<String> tagNameList, JSONObject tagMap, List<String> fieldNameList, List<String> fieldTypeList) {
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
        metadata.put("dbType",1);
        metadata.put("dbName",dbName);
        metadata.put("table",table);
        metadata.put("tag",tagMap);
        JSONArray tagName=JSONArray.fromObject(tagNameList);
        metadata.put("tagName",tagName);
        MetaDataService metaDataService=new MetaDataService();
        MongoCollection mongoCollection=metaDataService.openCol();
        metaDataService.saveMetaData(metadata.toString(),mongoCollection);
        metaDataService.close();
    }

    /*
     * batch Insert Method
     */

    public Boolean batchInsert(String path, String measurement, String dbName, Structure structure) throws Exception
    {
        System.out.println("batchInsert");
        String rentention="autogen";
        InfluxDB influxDB = null;
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
        //System.out.println("dbName:"+dbName);
        BufferedReader br = null;
        Long excuteTime=0L;
        try {
            //System.out.println("get Connection");
            influxDB = this.getConnection();
            System.out.println(influxDB.toString()+" "+dbName+" Opened");
            influxDB.enableBatch(this.batchNum, this.batchTime, TimeUnit.MILLISECONDS);
            influxDB.setDatabase(dbName);
            br = new BufferedReader(new FileReader(file));
            Integer startLine = structure.getStartLine();
            while (startLine > 1) {
                startLine--;
                br.readLine();
            }
            //Long time_s=System.currentTimeMillis();
            Long sysTime = 0L;
            String line = "";
            List<Pair<String, String>> header = structure.getFileStruct();
            String timeFormat = structure.getTimeFormat();
            System.out.println(structure.toString());
            //BatchPoints batchPoints= BatchPoints.database(dbName).build();
            while ((line = br.readLine()) != null && line!="") {
                if(sysTime%1000000==0) System.out.println(sysTime);
                line = line.replace("\"", "");
                String[] objList = line.split(",",-1);
                try {
                    String time = null;
                    time = String.valueOf(++sysTime);
                    Map<String, Double> SD = new HashMap<String, Double>();
                    Map<String, Long> SL = new HashMap<String, Long>();
                    Map<String, Boolean> SB = new HashMap<String, Boolean>();
                    Map<String, String> SS = new HashMap<String, String>();
                    Map<String, String> ST = new HashMap<String, String>();
                    for (int i = 0; i < header.size(); i++) {
                        if(objList[i].equals("")||objList[i].equals("NaN")||objList[i]==null) continue;
                        String K = header.get(i).getFirst();
                        String V = header.get(i).getSecond();
                        //System.out.println(objList[i]);
                        //System.out.println(sysTime.toString()+" "+String.valueOf(i)+" "+objList[i]);
                        if (V.equals("Boolean"))
                            SB.put(K, Boolean.valueOf(objList[i]));
                        if (V.equals("String"))
                            SS.put(K, objList[i]);
                        if (V.equals("Tag"))
                            ST.put(K, objList[i]);
                        if (V.equals("Long"))
                            SL.put(K, Long.valueOf(objList[i]));
                        if (V.equals("Double"))
                            SD.put(K, Double.valueOf(objList[i]));
                        if (V.equals("Time")) {
                            if(structure.getTimeFormat()==null||structure.getTimeFormat().equals("")) time=objList[i];
                            else time = dbUtil.parseTime(objList[i], timeFormat);
                        }
                    }

                    Point p = this.generatePoint(measurement, time, SL, SD, SS, SB, ST);
                    influxDB.write(p);

                    List<String> fieldNameList=new ArrayList<>();
                    List<String> fieldTypeList=new ArrayList<>();
                    for(int i=0;i<structure.getFileStruct().size();i++)
                    {
                        String _name=structure.getFileStruct().get(i).getFirst();
                        String _type=structure.getFileStruct().get(i).getSecond();
                        fieldNameList.add(_name);
                        fieldTypeList.add(_type);
                    }
                    updateMetaData(dbName,measurement,new ArrayList<>(),new JSONObject(),fieldNameList,fieldTypeList);
                    //System.out.println(p.toString());
                    //batchPoints.point(p);
                    //if(sysTime%1000==0) influxDB.write(batchPoints);
                } catch (Exception e) {
                    System.out.println("error occured when inserting data in influxUtil\n" + e);
                    System.out.println(sysTime);
                    return false;
                }
            }
            // influxDB.write(batchPoints);
            //  Long time_e=System.currentTimeMillis();
            // System.out.println("excuteTime:"+String.valueOf(time_e-time_s));
        } catch (Exception e) {
            System.out.println(e);
            return false;
        } finally {
            this.closeConnection(influxDB);
            if (br != null) br.close();

        }
        return true;
    }

    public Boolean batchInsert(String path, String measurement, String dbName) throws Exception{
        Structure structure = dbUtil.parseFile(path);
        return batchInsert(path,measurement,dbName,structure);
    }

    public Boolean batchInsert(String path, String measurement) throws Exception{
        return this.batchInsert(path, measurement, null);
    }

    /*
        private Boolean batchInsertHeaderCSV(String path, String measurement, String dbName) {
            Boolean Status = true;
            File csv = new File(path);
            String fileName = csv.getName();
            System.out.println(fileName);
            BufferedReader br = null;
            InfluxDB influxDB = null;
            try {
                //BatchPoints batchPoints=BatchPoints.database(dbName).build();
                influxDB = this.getConnection();
                influxDB.enableBatch(this.batchNum, this.batchTime, TimeUnit.MILLISECONDS);
                influxDB.setDatabase(dbName);
                br = new BufferedReader(new FileReader(csv));
                String line = br.readLine().replace("\"", "");
                String[] headerList = line.split(",");
                List<Integer> fIndex = new ArrayList<Integer>();
                List<Integer> tIndex = new ArrayList<Integer>();
                if(structure.hast
                Integer timeIndex = this.getTimeIndex(headerList);
                if (headerList.length == 1) timeIndex = -1;
                Long sysTime = 1L;
                for (int i = 0; i < headerList.length; i++) {
                    System.out.println(headerList[i]);
                    if (timeIndex == i) continue;
                    else if (isTag(headerList[i])) tIndex.add(i);
                    else fIndex.add(i);
                }

                while ((line = br.readLine()) != null) {
                    line = line.replace("\"", "");
                    Map<String, Object> ftmp = new HashMap<String, Object>();
                    Map<String, String> ttmp = new HashMap<String, String>();
                    String[] objList = line.split(",");
                    try {
                        for (Integer i : fIndex) ftmp.put(headerList[i], objList[i]);
                        for (Integer i : tIndex) ttmp.put(headerList[i], objList[i]);
                        String time = null;
                        if (timeIndex != -1) {
                            time = objList[timeIndex];
                            String fmt = this.getTimeFormat(time);
                            time = this.parseTime(time, fmt);
                        } else time = String.valueOf(sysTime++);
                        //System.out.println(time);
                        Point p = this.generatePoint(measurement, time, ftmp, ttmp);
                        influxDB.write(p);
                    } catch (Exception e) {
                        System.out.println("error occured when inserting data in influxUtil\n" + e);
                        this.closeConnection(influxDB);
                        br.close();
                        return false;
                    }
                    //batchPoints.point(p);
                    //if(time%1000==0) influxDB.write(batchPoints);
                }
                //influxDB.write(batchPoints);
            } catch (Exception e) {
                System.out.println(e);
                this.closeConnection(influxDB);
                return false;
            }
            try {
                this.closeConnection(influxDB);
                if (br != null) br.close();
            } catch (Exception e) {
                System.out.println("close reader buffer failed\n" + e);
            }
            return true;
        }

        private Boolean batchInsertHeaderCSV(String path, String measurement) {
            return this.batchInsertHeaderCSV(path, measurement, null);
        }


        private Boolean batchInsertCSV(String path, String measurement, String dbName) {
            System.out.println("bi");
            Boolean Status = true;
            File csv = new File(path);
            String fileName = csv.getName();
            System.out.println(fileName);
            if (dbName == null || dbName.equals(""))
                dbName = this.defaultDbName; //if dbName is null set dbName to default:"test"
            BufferedReader br = null;
            InfluxDB influxDB = null;
            try {
                //BatchPoints batchPoints=BatchPoints.database(dbName).build();
                //batchPoints.
                influxDB = this.getConnection();
                influxDB.enableBatch(this.batchNum, this.batchTime, TimeUnit.MILLISECONDS);
                influxDB.setDatabase(dbName);
                br = new BufferedReader(new FileReader(csv));
                String line = "";
                Long sysTime = 1L;
                while ((line = br.readLine()) != null) {
                    Map<String, Object> tmpField = new HashMap<String, Object>();

                    String[] objList = line.split(",");
                    String time = null;
                    if (objList.length != 1) {
                        time = objList[0];
                        String fmt = this.getTimeFormat(time);
                        time = this.parseTime(time, fmt);
                    } else time = String.valueOf(sysTime++);
                    if (objList.length == 1) tmpField.put("field1", objList[0]);
                    else {
                        for (int i = 1; i < objList.length; i++) {
                            String field = "field" + String.valueOf(i);
                            tmpField.put(field, objList[i]);
                        }
                    }
                    //System.out.println(time);
                    Point p = this.generatePoint(measurement, time, tmpField);
                    influxDB.write(p);
                    //batchPoints.point(p);
                    //if(time%1000==0) influxDB.write(batchPoints);
                }
                //influxDB.write(batchPoints);
                this.closeConnection(influxDB);
            } catch (Exception e) {
                System.out.println(e);
                this.closeConnection(influxDB);
                return false;
            }
            try {
                if (br != null) br.close();
            } catch (Exception e) {
                System.out.println("close reader buffer failed\n" + e);
            }
            if (influxDB != null) this.closeConnection(influxDB);
            return true;
        }

        /*
         * query by time range limit topK instances
         */

    public List<List<String>> query(String dbName, String q) {
        InfluxDB influxDB = getConnection();
        Query query = new Query(q, dbName);
        QueryResult result = influxDB.query(query);
        if (result == null) {
            //System.out.println("query failed,result is null");
            this.closeConnection(influxDB);
            return null;
        }
        List<List<String>> ans = new ArrayList<List<String>>();
        for (Result rs : result.getResults()) {
            List<Series> series = rs.getSeries();
            if (series == null) {
                //System.out.println("searching result is empty");
                this.closeConnection(influxDB);
                return null;
            }
            for (Series sr : series) {
                List<List<Object>> values = sr.getValues();
                List<String> columns = sr.getColumns();
                ans.add(columns);
                for (List<Object> ls : values) {
                    List<String> tl = new ArrayList<String>();
                    for (Object t : ls) tl.add(String.valueOf(t));
                    ans.add(tl);
                }
            }
        }
        this.closeConnection(influxDB);
        //showQueryResult(ans);
        return ans;
    }


    public List<List<String>> queryByTime(String dbName, String measurement, String startTime, String endTime, List<String> field, Integer topK) {
        if (dbName == null) {
            System.out.println("queryByTime dbName=null error");
            return null;
        }
        if (measurement == null) {
            System.out.println("queryByTime measurement=null error");
            return null;
        }
        InfluxDB influxDB = getConnection();
        System.out.println(influxDB.toString());
        StringBuilder q = new StringBuilder("SELECT");
        if (field == null || field.size() == 0) q = q.append(" *");
        else
            for (int i = 0; i < field.size(); i++) {
                if (i == 0) q.append(" ");
                else q.append(",");
                q.append(field.get(i));
            }
        q.append(" FROM " + measurement);
        if (startTime != null && endTime != null) q.append(" WHERE TIME>=" + startTime + " AND TIME<=" + endTime);
        if (startTime != null && endTime == null) q.append(" WHERE TIME>=" + startTime);
        if (startTime == null && endTime != null) q.append(" WHERE TIME<=" + endTime);
        if (topK != null) q.append(" LIMIT " + String.valueOf(topK));
        Query query = new Query(String.valueOf(q), dbName);
        QueryResult result = influxDB.query(query);
        if (result == null) {
            System.out.println("query failed");
            this.closeConnection(influxDB);
            return null;
        }
        List<List<String>> ans = new ArrayList<List<String>>();
        for (Result rs : result.getResults()) {
            List<Series> series = rs.getSeries();
            if (series == null) {
                System.out.println("searching result is empty");
                this.closeConnection(influxDB);
                return null;
            }
            for (Series sr : series) {
                List<List<Object>> values = sr.getValues();
                List<String> columns = sr.getColumns();
                ans.add(columns);
                for (List<Object> ls : values) {
                    List<String> tl = new ArrayList<String>();
                    for (Object t : ls) tl.add(String.valueOf(t));
                    ans.add(tl);
                }
            }
        }
        this.closeConnection(influxDB);
        //showQueryResult(ans);
        return ans;
		/*Query query = QueryBuilder.newQuery("SELECT * FROM cpu WHERE idle > $idle AND system > $system")
		        .forDatabase(dbName)
		        .bind("idle", 90)
		        .bind("system", 5)
		        .create();
		QueryResult results = influxDB.query(query);*/
    }

    public List<List<String>> queryByTime(String dbName, String measurement, String startTime, String endTime, List<String> field) {
        return this.queryByTime(dbName, measurement, startTime, endTime, field, null);
    }

    public List<List<String>> queryByTime(String dbName, String measurement, String startTime, String endTime) {
        return this.queryByTime(dbName, measurement, startTime, endTime, null);
    }

    public List<List<String>> queryByTime(String dbName, String measurement, Integer topK) {
        return this.queryByTime(dbName, measurement, null, null, null, topK);
    }

    public List<List<String>> queryByTime(String dbName, String measurement) {
        return this.queryByTime(dbName, measurement, null, null, null, null);
    }

    public List<List<String>> queryByTime(String dbName, String measurement, List<String> field) {
        return this.queryByTime(dbName, measurement, null, null, field, null);
    }


}
