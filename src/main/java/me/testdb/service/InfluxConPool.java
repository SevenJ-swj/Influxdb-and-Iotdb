package me.testdb.service;

import me.testdb.tool.InfluxConfig;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class InfluxConPool {

    //使用LinkedList集合存放数据库连接
    private static LinkedList<InfluxDB> connPool = new LinkedList<InfluxDB>();

    //在静态代码块中加载配置文件
    static{
        InfluxConfig influxConfig=new InfluxConfig();
        try {
            //数据库连接池的初始化连接数的大小
            int  InitSize = 15;
            //加载驱动
            String url=influxConfig.getUrl();
            for(int i = 0; i < InitSize; i++){
                InfluxDB influxDB=InfluxDBFactory.connect(url, new OkHttpClient.Builder()
                        .connectTimeout(300, TimeUnit.SECONDS)
                        .readTimeout(3600, TimeUnit.SECONDS));

                //将创建的连接添加的list中
                connPool.add(influxDB);
            }
            System.out.println("初始化数据库连接池，创建 " + InitSize +" 个连接，添加到池中");

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取数据库连接
     */
    public InfluxDB getConnection() throws Exception {
        if(connPool.size() > 0){
            //从集合中获取一个连接
            final InfluxDB conn = connPool.removeFirst();
            //返回Connection的代理对象
            return (InfluxDB) Proxy.newProxyInstance(InfluxConPool.class.getClassLoader(), conn.getClass().getInterfaces(), new InvocationHandler() {
                public Object invoke(Object proxy, Method method, Object[] args)
                        throws Throwable {
                    if(!"close".equals(method.getName())){
                        return method.invoke(conn, args);
                    }else{
                        connPool.add(conn);
                        System.out.println("返还连接给连接池");
                        System.out.println("池中连接数为 " + connPool.size());
                        return null;
                    }
                }
            });
        }else{
            throw new RuntimeException("数据库繁忙，稍后再试");
        }
    }

    public PrintWriter getLogWriter() throws Exception {
        return null;
    }

    public void setLogWriter(PrintWriter out) throws Exception {

    }

    public void setLoginTimeout(int seconds) throws Exception {

    }

    public int getLoginTimeout() throws Exception {
        return 0;
    }

    public Logger getParentLogger() throws Exception {
        return null;
    }

    public Object unwrap(Class iface) throws Exception {
        return null;
    }

    public boolean isWrapperFor(Class iface) throws Exception {
        return false;
    }

    public InfluxDB getConnection(String username, String password)
            throws Exception {
        return null;
    }
    
}
