package com.dataStore.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public class DataStoreManager {
	
	// Map containing all data
	private static Map<String, List<String>> dataStoreMap;
	// lock for data store map, for quick actions on map
	private static ReentrantLock dataStoreMapLock;
	// Map containing a lock for each key, so actions won't be made on same key at the same time
	private static Map<String, ReentrantLock> keyLockMap;
	// lock for key lock map, for quick actions
	private static ReentrantLock keyLockMapLock;
	
	// The data store directory (relative path)
	private static final String DATA_STORE_DIRECTORY = "data_store_directory";
	
	// Initialize the data store map from the file system
	public static void initDataStoreMap() throws Exception {
		dataStoreMap = new HashMap<String, List<String>>();
		dataStoreMapLock = new ReentrantLock();
		keyLockMap = new HashMap<String, ReentrantLock>();
		keyLockMapLock = new ReentrantLock();
		
		// make sure the data store directory exists
		File dataStoreDir = new File(DATA_STORE_DIRECTORY);
		if(!dataStoreDir.exists()) {
			dataStoreDir.mkdir();
		}
		
		// run on all files in data store directory
		// the file names represent key, and the content holds their values
		for(File keyFile: dataStoreDir.listFiles()) {
			String key = keyFile.getName();
			List<String> values = new ArrayList<String>();
			
			// read values from file
			BufferedReader br = new BufferedReader(new FileReader(keyFile));
			String value;
			while((value = br.readLine()) != null) {
				if(!value.isEmpty()) {
					values.add(value);
				}
			}
			
			// add key and values to data store map
			dataStoreMap.put(key, values);
			// add lock to key lock map
			keyLockMap.put(key, new ReentrantLock());
			
			br.close();
		}
	}
	
	// return all keys matching pattern
	public List<String> getAllKeys(String pattern){
		final Pattern p = Pattern.compile(pattern);
		// filter all file names (representing the keys) to return the ones matching the pattern
		File dataStoreDir = new File(DATA_STORE_DIRECTORY);
		String[] matchingKeys = dataStoreDir.list(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return p.matcher(name).matches();
			}
		});
		
		return new ArrayList<String>(Arrays.asList(matchingKeys));
	}
	
	// add a value to key K, from the right
	public void rightAdd(String K, String V) throws Exception {
		add(K, V, "R");
	}
	
	// add a value to key K, from the left
	public void leftAdd(String K, String V) throws Exception {
		add(K, V, "L");
	}
	
	private void add(String K, String V, String direction) throws Exception {
		// lock key K
		lockByKey(K);
		
		// lock map
		dataStoreMapLock.lock();
		// edit map
		List<String> values = dataStoreMap.get(K);
		if(values == null) {
			values = new ArrayList<String>();
		}
		if(direction.equals("L")) {
			// add value from left
			values.add(0, V);
		} else {
			// add value from right
			values.add(V);
		}
		dataStoreMap.put(K, values);
		// unlock map
		dataStoreMapLock.unlock();
		
		// write values to file system
		editFileSystem(K, values);
		
		// unlock key K
		unlockByKey(K);
	}
	
	// set key K with values V
	public void set(String K, List<String> V) throws Exception {
		// lock key K
		lockByKey(K);
		
		// lock map
		dataStoreMapLock.lock();
		// edit map
		dataStoreMap.put(K, V);
		// unlock map
		dataStoreMapLock.unlock();
		
		// write values to file system
		editFileSystem(K, V);
		
		// unlock key K
		unlockByKey(K);
	}
	
	// get(String K) – gets a list by its key
	public List<String> get(String K){
		List<String> values = new ArrayList<String>();
		
		// lock key K
		lockByKey(K);
		// lock map
		dataStoreMapLock.lock();
		// read values from map
		if(dataStoreMap.get(K) != null) {
			// copy the values in order to free the map
			values.addAll(dataStoreMap.get(K));
		}
		// unlock map
		dataStoreMapLock.unlock();
		// unlock key K
		unlockByKey(K);
		
		return values;
	}
	
	// lock by given key
	private void lockByKey(String key) {
		// lock key lock map
		keyLockMapLock.lock();
		// get key lock, add key if doesn't exist
		ReentrantLock keyLock = keyLockMap.get(key);
		if(keyLock == null) {
			keyLock = new ReentrantLock();
			keyLockMap.put(key, keyLock);
		}
		// unlock key lock map
		keyLockMapLock.unlock();
		
		// lock key lock
		keyLock.lock();
	}
	
	// unlock by given key
	private void unlockByKey(String key) {
		// lock key lock map
		keyLockMapLock.lock();
		// unlock key lock
		keyLockMap.get(key).unlock();
		// unlock key lock map
		keyLockMapLock.unlock();
	}
	
	// edit file system with given key and values
	private void editFileSystem(String key, List<String> values) throws Exception {
		// write the values to a file in the data store directory
		// with the key for its name
		PrintWriter pw = new PrintWriter(DATA_STORE_DIRECTORY + "/" + key);
		for(String value: values) {
			pw.println(value);
		}
		pw.close();
	}

}
