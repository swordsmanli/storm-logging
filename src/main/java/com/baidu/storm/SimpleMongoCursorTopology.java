package com.baidu.storm;

import static backtype.storm.utils.Utils.tuple;
import static java.lang.System.exit;
import static java.lang.System.setOut;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import com.sun.corba.se.spi.orbutil.fsm.Input;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

import com.baidu.storm.common.MongoToTupleFormatter;
import com.baidu.storm.bolt.MongoInsertBolt;
import com.baidu.storm.spout.MongoSpout;

class InsertHelper implements Runnable {

	public InsertHelper(String dbHost, int dbPort, String dbName, 
			String collectionName, CountDownLatch latch) {
		this.dbHost = dbHost;
		this.dbPort = dbPort;
		this.dbName = dbName;
		this.collectionName = collectionName;
		this.latch = latch;
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		DBCollection coll;
		try {
			coll = DBHelperMethod.getDBCollection(this.dbHost, this.dbPort, 
					this.dbName, this.collectionName);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MongoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//wait until storm main thread ready
		while(this.latch.getCount() != 0) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//insert something as you want
		
	}

	private String dbHost;
	private int dbPort;
	private String dbName;
	private String collectionName;
	private CountDownLatch latch;
	
}

class DBHelperMethod {
	public static DBCollection getDBCollection(String mongHost, int mongoPort, 
			String dbName, String collectionName) throws UnknownHostException, MongoException {
		DBCollection collection;
		Mongo mongo = new Mongo(mongHost, mongoPort);
		DB db = mongo.getDB(dbName);
		collection = db.getCollection(collectionName);
		return collection; 
	}
	
	public static DBCollection createCappedDBCollection(String mongHost, int mongoPort, 
			String dbName, String collectionName) throws UnknownHostException, MongoException {
		DBCollection collection;
		Mongo mongo = new Mongo(mongHost, mongoPort);
		DB db = mongo.getDB(dbName);
		collection = db.createCollection(collectionName, 
				new BasicDBObject("capped", true).append("size", 10000000));
		
		return collection;
	}
}

public class SimpleMongoCursorTopology {

	/**
	 * @param args
	 * @throws MongoException 
	 * @throws UnknownHostException 
	 */
	
	
	
	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		final String mongoHost = "";
		final int mongoPort = ;
		final String spoutDbName = "";
		final String spoutCollectionName = "";
		final String boltDbName = "";
		final String boltCollectionName = "";
		
		//main thread to await all the other threads
		CountDownLatch latch = new CountDownLatch(1);
		InsertHelper inserter = new InsertHelper(mongoHost, mongoPort, boltDbName, 
				boltCollectionName, latch);
		new Thread(inserter).start();
		
		//get a bolt mongdb collection
		final DBCollection coll = DBHelperMethod.createCappedDBCollection(mongoHost, mongoPort, 
				boltDbName, boltCollectionName);
		
		
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * wired java function
		 * Anonymous Inner Class initialization
		 * must override unimplemented functions
		 */
		MongoSpout spout = new MongoSpout(mongoHost, mongoPort, spoutDbName, 
				spoutCollectionName, new BasicDBObject()) {


			private static final long serialVersionUID = 1L;
			
			@Override
			public void declareOutputFields(OutputFieldsDeclarer declarer) {
				// TODO Auto-generated method stub
				declarer.declare(new Fields("Document"));
			}

			@Override
			public List<Object> dbObjectToStormTuple(DBObject message) {
				// TODO Auto-generated method stub
				return tuple(message);
			}

			@Override
			public void activate() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void deactivate() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public Map<String, Object> getComponentConfiguration() {
				// TODO Auto-generated method stub
				return null;
			}
			
		};	
		
		MongoToTupleFormatter formatter = new MongoToTupleFormatter() {
			
			@Override
			public DBObject mapTupleToDBObject(DBObject object, Tuple tuple) {
				//do average imas timeout caculation
				int innerCount = 0;
				int imas_time = 0;
				JSONObject json = (JSONObject) tuple.getValueByField("Document");
				if(json.containsKey("logtime") && json.containsKey("tt")) {
					String logtime = json.get("logtime").toString();
					String tt = json.get("tt").toString();
					imas_time += Integer.parseInt(tt);
					innerCount += 1;
				}
				return BasicDBObjectBuilder.start()
						.add("sum", )
						.add(key, val)
						
						

			}
			
		};
		
		builder.setSpout("1", spout, 10);
		
		Config conf = new Config();
		conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("mongoStorm", conf, builder.createTopology());
		
/*		Runnable writer = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				for(int i = 0; i < 50; i++) {
					final BasicDBObject doc = new BasicDBObject("_id", i);
					doc.put("ts", new Date());
					coll.insert(doc);
				}
				
				DBCursor cursor = coll.find();
				int count = 0;
				try {
					while(cursor.hasNext()) {
						count += 1;
						DBObject tmp = cursor.next();
						System.out.println(tmp.get("logtime").toString());
						if (count > 10) break;
					}
				}finally {
					cursor.close();
				}
			}
			
		}; 
		
		new Thread(writer).start();
		
		Utils.sleep(10000);
		cluster.shutdown();*/
		
	}

}
