package me.testdb.service;



import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MetaDataService {

    private MongoClient mongoClient;

    public MongoClient getConnection() {

        MongoCredential credential = MongoCredential.createCredential("dsm", "workflow", "Passw0rd".toCharArray());
       // return (new MongoClient("10.141.212.70"));

        return (new MongoClient(new ServerAddress("10.141.212.70"), Arrays.asList(credential)));
    }

    private void closeConnection(MongoClient mongoClient) {
        mongoClient.close();
    }

    private MongoCollection getCollection(MongoClient mongoClient) {
        String DB_NAME = "workflow";
        String DB_COL = "metaData";
        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(DB_NAME);
        // 获取集合
        MongoCollection mongoCollection = mongoDatabase.getCollection(DB_COL);
        if (mongoCollection == null) mongoDatabase.createCollection(DB_COL);
        mongoCollection = mongoDatabase.getCollection(DB_COL);

        return mongoCollection;
    }

    public MongoCollection openCol()
    {
        mongoClient = getConnection();
        MongoCollection mongoCollection = getCollection(mongoClient);
        return mongoCollection;
    }
    public void close()
    {
        mongoClient.close();
    }
    public void saveMetaData(String metadata,MongoCollection mongoCollection) {
        //MongoClient mongoClient = getConnection();
        //MongoCollection mongoCollection = getCollection(mongoClient);
        //FindIterable<Document> findIterable = mongoCollection.find(Filters.eq("id", metaData.getId()));
        //MongoCursor<Document> mongoCursor = findIterable.iterator();
        Document newDoc = Document.parse(metadata);
        mongoCollection.insertOne(newDoc);
        //closeConnection(mongoClient);
    }

}
/*
    public void updateMetaData(MetaData metaData){
        MongoClient mongoClient=getConnection();
        MongoCollection mongoCollection = getCollection(mongoClient);
        mongoCollection.updateOne(Filters.eq("id",  metaData.getId()), new Document("$set",Document.parse(metaData.toString())));

        closeConnection(mongoClient);
    }

    public MetaData findByIdAndVar(String id,String var){
        MongoClient mongoClient=getConnection();
        MongoCollection mongoCollection = getCollection(mongoClient);
        FindIterable<Document> findIterable = mongoCollection.find(new Document("id",id).append("aliasName",var));
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        MetaData metaData=null;
        if (mongoCursor.hasNext()) {
            Document doc = mongoCursor.next();
            metaData=new MetaData();
            metaData.setId(doc.getString("id"));
            metaData.setFieldName((List<String>)doc.get("fieldName"));
            metaData.setAliasName(doc.getString("aliasName"));
            metaData.setColumnNum(doc.getInteger("columnNum"));
            metaData.setDescription(doc.getString("description"));
            metaData.setDatabase(doc.getString("database"));
            metaData.setType(doc.getInteger("type"));
            metaData.setTable(doc.getString("table"));
            metaData.setRowNum(doc.getInteger("rowNum"));
            metaData.setFieldType((List<String>)doc.get("fieldType"));
        }
        closeConnection(mongoClient);
        return metaData;
    }

}/*

        // 显示所有数据库名字
//        mongoClient.listDatabaseNames().forEach((Block<String>) name -> {
//            System.out.println("DB_NAME: " + name);
//            if (name.equals(DB_NAME)) {
//                mongoClient.dropDatabase(DB_NAME);
//                System.out.println("DB " + name + " Found ======================");
//            }
//        });

  //插入文档
        Document document = new Document("title", "MongoDB").append("description", "database").append("likes", 100);
        List<Document> documents = new ArrayList<>();
        documents.add(document);
        documents.add(new Document("title", "HBase").append("description", "database2").append("likes", 90));
        documents.add(new Document("title", "Redis").append("description", "database3").append("likes", 80));
        documents.add(new Document("title", "Redis").append("description", new Document("test","qiantao").append("id",3)).append("likes", 80));
        List<String> att=new ArrayList<>();
        att.add("abc");
        att.add("but");
        documents.add(new Document("title", "Redis").append("description", new Document("test",att).append("id",3)).append("likes", 80));
        mongoCollection.insertMany(documents);
        // 更新文档
        mongoCollection.updateMany(Filters.eq("likes", 100), new Document("$set",new Document("likes",200)));
        Filters.eq("likes",10);
        // 删除符合条件的第一个文档
        mongoCollection.deleteOne(Filters.eq("likes", 80));
        mongoCollection.deleteMany(new Document("likes",80).append("title","Redis"));
        // 删除所有符合条件的文档
//        mongoCollection.deleteMany (Filters.eq("likes", 80));

        System.out.println("\nShow the documents .......................");

        // 查看所有文档
        //mongoCollection.find().forEach(((Block<Document>) doc -> System.out.println(doc)));

        // OR view collections like this way
        FindIterable<Document> findIterable = mongoCollection.find();
        MongoCursor<Document> mongoCursor = findIterable.iterator();
        while(mongoCursor.hasNext()){
            System.out.println(mongoCursor.next().toJson());
        }

        mongoClient.close();
    }
 */