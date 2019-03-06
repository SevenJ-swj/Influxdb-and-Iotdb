package me.testdb.service;

import org.influxdb.InfluxDB;

public class InfluxDBUtil {
    //数据库连接池
    private static InfluxConPool  connPool = new InfluxConPool();

    /**
     * 从池中获取一个连接
     * @return
     * @throws Exception
     */
    public static InfluxDB getConnection() throws Exception{
        return connPool.getConnection();
    }

    /**
     * 关闭连接
     * @param conn
     * @throws Exception
     */
    public static void CloseConnection(InfluxDB conn){

        //关闭连接
        if(conn != null){
            conn.close();
        }
    }
}
