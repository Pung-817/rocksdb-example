package org.example;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.RestExpress;
import org.terarkdb.*;
import org.terarkdb.HdfsEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
*/

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

/**
 * 
 The given code block contains a simple counter using rocksdb's merge operator
 
 the Counters Fails for the following Cases
 
 $ ab -r -k -n 10000 -c 1000 http://localhost:9009/increment

 $ curl http://localhost:9009/get                                                                                                                                                           

 Current RocksDB Count is 137   (Expected value is 10000)
 Current AtomicLong Count is 10000

 $ curl http://localhost:9009/reset

 Current RocksDB Count is 0
 Current AtomicLong Count is 0

 $ ab -r -k -n 10000 -c 1000 http://localhost:9009/batchIncrement

 $ curl http://localhost:9009/get

 Current RocksDB Count is 16   (Expected value is 10000)
 Current AtomicLong Count is 10000
 
 */
public class RocksDBConnector {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBConnector.class);

    private  static  RocksDB db;

    private static ColumnFamilyHandle cfHandle;

    private static long startTime;
    private static long endTime;

    private  RocksDBConnector(){}

    private final static RocksDBConnector instance = new RocksDBConnector();

    public static void main(String[] args) {
        try {
            initializeRocksDb();
            db_load();
            testTBaseTools();
        } catch (IOException e) {
            System.out.print(e);
            LOG.info(e.getMessage());
        }catch(InterruptedException e){
            System.out.print(e);
            LOG.info(e.getMessage());
        } catch (RocksDBException e) {
            System.out.print(e);
            LOG.info(e.getMessage());
        }
    }

    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
        int number=random.nextInt(62);
        sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    private static void db_load()throws RocksDBException, InterruptedException{
        for(int count = 0; count < 8; ++count) {
            if(count == 4){
                Thread.sleep(2000);
                startTime = System.currentTimeMillis() / 1000L;
                System.out.println("start load time: " + String.valueOf(startTime));
            }
            for(int i =0 ;i< 8;i++){
                String key = getRandomString(8);
                String value = getRandomString(i);
                //for(int j = 0;j < 1000;j++){ 
                db.put(key.getBytes(),value.getBytes());
                //}
                //System.out.println("finished load " + String.valueOf(i));
            }
            db.flush(new FlushOptions().setWaitForFlush(true));
            System.out.println("finished load sst " + String.valueOf(count));
        }
        System.out.println("finished load");
        Thread.sleep(2000);
        endTime = System.currentTimeMillis() / 1000L;
        System.out.println("end load time: " + String.valueOf(endTime));
        System.out.println("load time: " + String.valueOf(endTime - startTime));
        //db.compactRange();
    }

    private static void testTBaseTools() throws RocksDBException {
        String separator = "======================================================";
        String newline = System.lineSeparator();

        System.out.println(newline + separator);
        System.out.println("               TBaseTools Test Suite");
        System.out.println(separator);

        cfHandle = db.getDefaultColumnFamily();

        // Test 1: getTablesByCreationTime
        System.out.printf("%n[TEST] Running: getTablesByCreationTime%n");
        System.out.printf("[INFO] Searching for tables between %d and %d%n", startTime, endTime);
        List<byte[]> tableNames = TBaseTools.getTablesByCreationTime(cfHandle, startTime, endTime);

        if (tableNames == null || tableNames.size() < 4) {
            int foundCount = (tableNames == null) ? 0 : tableNames.size();
            System.out.printf("[FAIL] getTablesByCreationTime: Expected at least 4 tables, but found %d.%n", foundCount);
            System.out.println(separator);
            return;
        }

        System.out.printf("[PASS] getTablesByCreationTime: Found %d tables.%n", tableNames.size());
        for (int i = 0; i < tableNames.size(); i++) {
            System.out.printf("       - Table %d: %s%n", i + 1, new String(tableNames.get(i)));
        }

        // Test 2: getTableCreationTime
        byte[] testTableNameBytes = tableNames.get(3);
        String testTableNameStr = new String(testTableNameBytes);
        System.out.printf("%n[TEST] Running: getTableCreationTime%n");
        System.out.printf("[INFO] Checking creation time for table: %s%n", testTableNameStr);

        long creationTime = TBaseTools.getTableCreationTime(cfHandle, testTableNameStr);
        System.out.printf("[INFO] Reported creation time: %d%n", creationTime);

        if (creationTime >= startTime && creationTime <= endTime) {
            System.out.println("[PASS] getTableCreationTime: Creation time is within the expected range.");
        } else {
            System.out.printf("[FAIL] getTableCreationTime: Creation time %d is outside the expected range [%d, %d].%n", creationTime, startTime, endTime);
        }

        // Test 3: getTableIterator
        System.out.printf("%n[TEST] Running: getTableIterator%n");
        System.out.printf("[INFO] Creating iterator for table: %s%n", testTableNameStr);
        RocksIterator iterator = null;
        try {
            iterator = TBaseTools.getTableIterator(db, cfHandle, testTableNameStr);
            if (iterator == null) {
                System.out.println("[FAIL] getTableIterator: Iterator is null.");
                return;
            }

            int entryCount = 0;
            System.out.println("[INFO] Iterating through key-value pairs:");
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                System.out.printf("       - Key: %-10s | Value: %s%n", new String(iterator.key()), new String(iterator.value()));
                entryCount++;
            }

            if (entryCount > 0) {
                System.out.printf("[PASS] getTableIterator: Found and iterated over %d entries.%n", entryCount);
            } else {
                System.out.println("[WARN] getTableIterator: Iterator is valid, but no entries were found in the table.");
            }
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }

        System.out.println(newline + separator);
        System.out.println("              TBaseTools Test Finished");
        System.out.println(separator);
    }

    private static void initializeRocksDb() throws RocksDBException,UnsupportedEncodingException{
            RocksDB.loadLibrary();

            final String hdfsUri = "hdfs://hostname:port";

            final HdfsEnv hdfsEnv = new HdfsEnv(hdfsUri);
            
            Options options = new Options().setCreateIfMissing(true);
            options.setEnv(hdfsEnv);
            options.setDisableAutoCompactions(true);
            options.setMergeOperatorName("uint64add");
            options.setMaxBackgroundFlushes(1);
            options.setWriteBufferSize(50L);
            options.setBlobSize(256);
            options.setCreateMissingColumnFamilies(true);

            if (db == null) {
                db = RocksDB.open( options, hdfsUri + "/username/tmp/testdata");
            }
    }
}
