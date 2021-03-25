package com.xbinb.lggame.sdk.disk;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xbinb.comm.cache.SystemConfig;
import com.xbinb.comm.util.StringUtil;


/**
 *  of disk-based queue design and implementation, DiskQueuePool is mainly refined from
 *  his brilliant work.
 *  
 *  
 */
public class DiskQueuePool implements Closeable{ 
  //  static final Logger log = Logger.getLogger(DiskQueuePool.class);
    
	static Logger log = LoggerFactory.getLogger(DiskQueuePool.class);

	private final BlockingQueue<String> deletingQueue = new LinkedBlockingQueue<String>();
    
    private String fileBackupPath;
    private Map<String, DiskQueue> queueMap;
    private ScheduledExecutorService syncService;


    private static DiskQueuePool instance;
    
    public synchronized static   void init(String filePath){
    	instance = new DiskQueuePool(filePath);
    }
    public static DiskQueuePool getInstanceStart(String filePath){
    	if(instance == null){
    		init(filePath);
    	}
    	return instance; 
    }
    public static DiskQueuePool getInstance(){
    	if(instance == null){
    		String mqPath = SystemConfig.getInstance().get("mq_path" ); 
    		
    		init(mqPath);
    	}
    	return instance; 
    }
    public DiskQueuePool(String fileBackupPath) {
        this.fileBackupPath = fileBackupPath;
        File fileBackupDir = new File(fileBackupPath);
        if (!fileBackupDir.exists() && !fileBackupDir.mkdir()) {
            throw new IllegalArgumentException("can not create directory");
        }
        this.queueMap = scanDir(fileBackupDir);
        this.syncService = Executors.newSingleThreadScheduledExecutor();
        this.syncService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (DiskQueue q : queueMap.values()) {
                    q.sync();
                }
                deleteBlockFile();
            }
        }, 1000, 10000, TimeUnit.MILLISECONDS);
    }
    
    public Map<String, DiskQueue> getQueryMap(){
    	return queueMap;
    }
 
    private void deleteBlockFile() {
        String blockFilePath = null;
        while( (blockFilePath = deletingQueue.poll()) != null){
        	blockFilePath = blockFilePath.trim();
        	if(blockFilePath.equals("")) continue;
        	log.info(String.format( "Delete File[%s]", blockFilePath));   
            File delFile = new File(blockFilePath);
            try {
                if (!delFile.delete()) {
                    log.warn(String.format("block file:%s delete failed", blockFilePath));
                }
            } catch (SecurityException e) {
                log.error("security manager exists, delete denied");
            } 
        }
    }
 
    public void toClear(String filePath) {
        deletingQueue.add(filePath);
    }
 
    private Map<String, DiskQueue> scanDir(File fileBackupDir) {
        if (!fileBackupDir.isDirectory()) {
            throw new IllegalArgumentException("it is not a directory");
        }
        Map<String, DiskQueue> queues = new HashMap<String, DiskQueue>();
        File[] indexFiles = fileBackupDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return DiskQueueIndex.isIndexFile(name);
            }
        });
        if (indexFiles != null && indexFiles.length> 0) {
            for (File indexFile : indexFiles) {
                String queueName = DiskQueueIndex.parseQueueName(indexFile.getName());
                queues.put(queueName, new DiskQueue(queueName, this));
            }
        }
        return queues;
    }
 
    
    public void close() throws IOException {
        this.syncService.shutdown();
        for (DiskQueue q : queueMap.values()) {
            q.close();
        }
        while (!deletingQueue.isEmpty()) {
            deleteBlockFile();
        }
    }

    public synchronized DiskQueue getDiskQueue(String queueName) {
        if (queueName==null || queueName.trim().equals("")) {
            throw new IllegalArgumentException("empty queue name");
        }
        if (queueMap.containsKey(queueName)) {
            return queueMap.get(queueName);
        }
        DiskQueue q = new DiskQueue(queueName, this);
        queueMap.put(queueName, q);
        return q;
    }
    
    public String getFileBackupPath(){
    	return this.fileBackupPath;
    }
    
    
 
}