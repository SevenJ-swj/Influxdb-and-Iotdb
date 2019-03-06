package me.testdb.tool;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

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

public class DBUtil {
    /*
     * Supported Time Format;
     */
    private List<String> supportTimeFormat = new ArrayList<String>(
            Arrays.asList
                    (
                            "yyyy-MM-dd HH:mm:ss",
                            "yyyy-MM-dd HH:mm:ss.S",
                            "yyyy-MM-dd HH:mm:ss.SS",
                            "yyyy-MM-dd HH:mm:ss.SSS",
                            "yyyy-MM-dd'T'HH:mm:ss'Z'",
                            "yyyy-MM-dd'T'HH:mm:ss.S'Z'",
                            "yyyy-MM-dd'T'HH:mm:ss.SS'Z'",
                            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                            "yyyy/M/dd HH:mm",
                            "yyyy/M/d HH:mm",
                            "yyyy/MM/d HH:mm",
                            "yyyy/MM/dd HH:mm",
                            "yyyy/M/dd H:mm",
                            "yyyy/M/d H:mm",
                            "yyyy/MM/d H:mm",
                            "yyyy/MM/dd H:mm",
                            "yyyy/MM/dd HH:mm:ss",
                            "yyyy/MM/dd HH:mm:ss.S",
                            "yyyy/MM/dd HH:mm:ss.SS",
                            "yyyy/MM/dd HH:mm:ss.SSS",
                            "yyyy/MM/dd-HH:mm:ss",
                            "yyyy/MM/dd-HH:mm:ss.S",
                            "yyyy/MM/dd-HH:mm:ss.SS",
                            "yyyy/MM/dd-HH:mm:ss.SSS"
                    )
    );

    Map<String, String> splitPattern = new HashMap<String, String>() {
        {
            put("csv", ",");
            put("tsv", "\t");
        }
    };
    /*
     * Supported Time Header Format
     * if not added set the first column as time stamp;
     * May be added in the future
     * When judging,Uppercase Will Transform to Lowercase;
     */
    //private final List<String> supportTimeHeader = new ArrayList<>(Arrays.asList("wait to add", "now empty"));
    private final Map<String, Boolean> supportTimeHeader = new HashMap<String, Boolean>() {
        {
            put("time", true);
            put("time_stamp", true);
            put("timestamp", true);
            put("stamp", true);
            put("timestamps", true);
        }
    };
    //Arrays.asList("time", "timestamp","stamp");


    public void showQueryResult(List<List<String>> influxQueryRes) {
        for (List<String> stringList : influxQueryRes) {
            for (String str : stringList) {
                System.out.print(str + " ");
            }
            System.out.print("\n");
        }
        System.out.println("end");
    }

    /*
     * The Format of Query Result of Time is:
     * 2018-11-12T14:06:27.000Z
     * And to Query By Time Range Needs Time stamps in microSeconds;
     * Exceeding Long type Need to process;
     */
    public String stampToInfluxDate(Long ts) {
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                .withZone(ZoneId.of("UTC"));
        // LocalDateTime time =LocalDateTime.ofEpochSecond(ts/1000,0,ZoneOffset.ofHours(0));
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.ofHours(0));
        return time.format(formatter);
    }

    public String stampToDefDate(Long ts, String format) {
        if (format.equals("") || format == null) return ts.toString();
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern(format)
                .withZone(ZoneId.of("UTC"));
        // LocalDateTime time =LocalDateTime.ofEpochSecond(ts/1000,0,ZoneOffset.ofHours(0));
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.ofHours(0));
        return time.format(formatter);
    }

    public String influxDateToStamp(String s) {
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .withZone(ZoneId.of("UTC"));
        LocalDateTime date = LocalDateTime.parse(s, formatter);
        long ts = date.toInstant(ZoneOffset.of("+0")).toEpochMilli();
        return String.valueOf(ts) + "000000";
    }

    public String scaleTime(String yyyy, String MM, String dd, String HH, String mm, String ss, String sss) {
        String str = yyyy + "-" + MM + "-" + dd + 'T' + HH + ":" + mm + ":" + ss + "." + sss + "Z";
        return str;
    }

    public String scaleTime(String yyyy, String MM, String dd, String HH, String mm, String ss) {
        String str = yyyy + "-" + MM + "-" + dd + 'T' + HH + ":" + mm + ":" + ss + "000Z";
        return str;
    }

    //2013-10-02 08:00:08
    /*
     * parsing time of some type to millisecond stamp
     */
    public String parseTime(String time, String type) {
        if (type == "" || type == null) return time;
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern(type)
                .withZone(ZoneId.of("UTC"));
        Long ts = null;
        LocalDateTime date = null;
        try {
            date = LocalDateTime.parse(time, formatter);
            ts = date.toInstant(ZoneOffset.of("+0")).toEpochMilli();
        } catch (Exception e) {
            System.out.println("Parsing Time Error in InfluxDB Util");
        }
        return String.valueOf(ts);
    }

    //"yyyy-MM-dd HH:mm:ss"
    public Boolean judgeTimeFormat(String time, String type) {
        DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern(type)
                .withZone(ZoneId.of("UTC"));
        try {
            LocalDateTime.parse(time, formatter);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String getTimeFormat(String time) {
        String ans = null;
        for (String format : supportTimeFormat) {
            if (judgeTimeFormat(time, format)) ans = format;
        }
        // if (ans == null) System.out.println("error:no supported time format found!");
        return ans;
    }

    public Boolean isTag(String obj) {
        if (obj == null || obj == "") return false;
        String[] sList = obj.split(":");
        if (sList.length > 1 && sList[0].equals("tag")) return true;
        return false;
    }

    public Boolean isTime(String obj) {
        if (obj == null || obj == "") return false;
        obj = obj.toLowerCase();
        if (supportTimeHeader.get(obj) != null) return true;
        return false;
    }

    private Integer getTimeIndex(String[] obj) {
        for (int i = 0; i < obj.length; i++) {
            if (supportTimeHeader.get(obj[i]) != null) return i;
        }
        return 0;
    }

    private String getFileSplit(String fileFormat) {
        return splitPattern.get(fileFormat);
    }

    private String getDataType(String s) {
        String timeFormat = getTimeFormat(s);
        if (timeFormat != null) return "Time";
        if (s.toLowerCase().equals("true") || s.toLowerCase().equals("false")) return "Boolean";
        try {
            Long data = Long.valueOf(s);
            return "Long";
        } catch (Exception e) {
        }
        try {
            Double data = Double.valueOf(s);
            return "Double";
        } catch (Exception e) {
        }
        return "String";
    }

    public Structure parseFile(String path) {
        // path="E:\\data\\algdata\\bending1\\dataset1.csv";
        File file = null;
        String fileFormat = null;
        try {
            file = new File(path);  // file path
            String[] fileinfo = file.getName().split("\\.");
            System.out.println(file.getName());
            fileFormat = fileinfo[fileinfo.length - 1];
            System.out.println("file format:" + fileFormat);
        } catch (Exception e) {
            System.out.println("open file failed,please check path\n" + e);
            // return null;
        }
        String splitReg = getFileSplit(fileFormat);
        splitReg = ",";
        BufferedReader br = null;
        Integer hasHeader = -1;
        List<String> header = new ArrayList<String>();
        List<String> datas = new ArrayList<String>();
        Integer ColumnNum = 0;
        Integer Step = 150;
        Integer curRow = 0;
        Integer startLine = 0;
        String timeformat = "";
        try {
            br = new BufferedReader(new FileReader(file));
            String preLine = null;
            //Object ts=br.lines().limit(10).toArray();
            String line = br.readLine().replace("\"", "");
            curRow++;
            String pattern[] = line.split(splitReg, -1);
            ColumnNum = pattern.length;
            Boolean firstMatch = true;
            preLine = line;
            while ((line = br.readLine()) != null && Step-- > 0) {
                curRow++;
                //if(Step==30) System.out.println(line);
                line = line.replace("\"", "");
                pattern = line.split(splitReg, -1);
                Integer newColNum = pattern.length;
                //System.out.println(line);
                //System.out.println(curRow+" clear  newCol"+newColNum);
                //System.out.println(String.valueOf(newColNum)+' '+String.valueOf(ColumnNum));
                if ((newColNum.equals(ColumnNum)) && firstMatch) {
                    String[] prePattern = preLine.split(splitReg, -1);
                    Boolean flag = false;
                    for (String obj : prePattern) {
                        if (!obj.toLowerCase().equals(obj.toUpperCase()))
                            flag = true;
                    }
                    if (flag) {
                        hasHeader = curRow - 1;
                        startLine = curRow;
                        for (String obj : prePattern) header.add(obj);
                    } else startLine = curRow - 1;
                    firstMatch = false;
                } else if ((newColNum.equals(ColumnNum))) {
                    datas.add(line);
                } else {
                    datas.clear();
                    firstMatch = true;
                    hasHeader = -1;
                    header.clear();
                    ColumnNum = newColNum;
                }
                preLine = line;
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            try {
                br.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        Integer fieldNum = datas.get(0).split(splitReg, -1).length;
        int[][] typeCnt = new int[fieldNum][6];
        for (String a : datas) {
            //System.out.println(a);
            String[] pattern = a.split(splitReg, -1);
            for (Integer i = 0; i < fieldNum; i++) {
                if (pattern[i].equals("") || pattern[i] == null) continue;
                String resType = getDataType(pattern[i]);
                if (resType.equals("Boolean")) typeCnt[i][0]++;
                if (resType.equals("Long"))
                    typeCnt[i][1]++;
                if (resType.equals("Double")) typeCnt[i][2]++;
                if (resType.equals("String")) typeCnt[i][3]++;
                if (resType.equals("Time")) {
                    typeCnt[i][4]++;
                    timeformat = getTimeFormat(pattern[i]);
                }
            }
        }
        Boolean hasTime = false;
        List<String> type = new ArrayList<String>();
        for (Integer i = 0; i < fieldNum; i++) {
            // Integer currentMax=-1,id=-1;
            if (typeCnt[i][4] != 0) {
                type.add("Time");
                hasTime = true;
            } else if (typeCnt[i][2] != 0) type.add("Double");
            else if (typeCnt[i][1] != 0) type.add("Long");
            else if (typeCnt[i][0] != 0) type.add("Boolean");
            else type.add("String");
        }


        if (hasHeader == -1) {
            for (Integer i = 0; i < fieldNum; i++) {
                header.add("auto_field" + i);
            }
        }
        List<Pair<String, String>> fileStruct = new ArrayList<Pair<String, String>>();
        for (Integer i = 0; i < fieldNum; i++) {
            if (hasTime == false && isTime(header.get(i))) {
                if (typeCnt[i][0] == 0 && typeCnt[i][2] == 0 && typeCnt[i][3] == 0) {
                    type.set(i, "Time");
                    hasTime = true;
                }
            }
            if (isTag(header.get(i))) {
                header.set(i, header.get(i).substring(4));
                type.set(i, "Tag");
            }
            Pair<String, String> pair = new Pair();
            pair.make_pair(header.get(i), type.get(i));
            fileStruct.add(pair);
        }
        Structure structure = new Structure();
        structure.setFileStruct(fileStruct);
        structure.setHasHeader(hasHeader);
        structure.setHasTime(hasTime);
        structure.setTimeFormat(timeformat);
        structure.setStartLine(startLine);
        System.out.println(structure.toString());
        return structure;
    }

}
