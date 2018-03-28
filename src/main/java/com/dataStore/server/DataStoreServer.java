package com.dataStore.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class DataStoreServer {
	
	// port for client server communication
	private static final Integer PORT = 9090;
	
	public static void main(String[] args) throws Exception {
		// initialize the data store map on startup
		DataStoreManager.initDataStoreMap();
		System.out.println("Data Store server is up.");
		ServerSocket serverSocket = new ServerSocket(PORT);
		try {
			while(true) {
				new DataStoreClientHandler(serverSocket.accept()).start();
			}
		} finally {
			serverSocket.close();
		}
	}
	
	// class to handle each client connection
	private static class DataStoreClientHandler extends Thread {
		private Socket socket;
		DataStoreManager dataStoreManager = new DataStoreManager();
		
		// some constants
		private final String EXIT_MESSAGE = "EXIT";
		private final String DONE_MESSAGE = "DONE";
		private final String ERROR_MESSAGE = "ERROR";
		private final String SEPARATOR = "DATA_STORE_SEPARATOR";
		private final String COMMA = "DATA_STORE_COMMA";
		private final String GET_ALL_KEYS_METHOD = "getAllKeys";
		private final String RIGHT_ADD_METHOD = "rightAdd";
		private final String LEFT_ADD_METHOD = "leftAdd";
		private final String SET_METHOD = "set";
		private final String GET_METHOD = "get";
		
		public DataStoreClientHandler(Socket socket) {
			// new connection
			this.socket = socket;
		}
		
		public void run() {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
				
				while(true) {
					String input = br.readLine();
					if(input == null || input.equals(EXIT_MESSAGE)) {
						break;
					}
					pw.println(handleClienRequest(input));
				}
				
			} catch(Exception e) {
				// exception while handling request from client
			} finally {
				try {
					socket.close();
				} catch (Exception e) {
					// exception while closing socket
				}
			}
		}
		
		// parse the request and execute
		private String handleClienRequest(String input) {
			String[] clientRequest = input.split(SEPARATOR);
			
			try {
				String requestMethod = clientRequest[0];
				if(requestMethod.equals(GET_ALL_KEYS_METHOD)) {
					// get all keys that match pattern request
					String pattern = clientRequest[1];
					List<String> matchingKeys = dataStoreManager.getAllKeys(pattern);
					return DONE_MESSAGE + SEPARATOR + matchingKeys.toString();
				} else if(requestMethod.equals(RIGHT_ADD_METHOD)) {
					// add value from the right to given key request
					String key = clientRequest[1];
					String value = clientRequest[2];
					dataStoreManager.rightAdd(key, value);
					return DONE_MESSAGE;
				} else if(requestMethod.equals(LEFT_ADD_METHOD)) {
					// add value from the left to given key request
					String key = clientRequest[1];
					String value = clientRequest[2];
					dataStoreManager.leftAdd(key, value);
					return DONE_MESSAGE;
				} else if(requestMethod.equals(SET_METHOD)) {
					// set values to given key request
					String key = clientRequest[1];
					List<String> values = new ArrayList<String>();
					if(clientRequest.length == 3) {
						String[] valuesArray =  clientRequest[2].split(COMMA);
						for(String value: valuesArray) {
							if(!value.isEmpty()) {
								values.add(value);
							}
						}
					}
					dataStoreManager.set(key, values);
					return DONE_MESSAGE;
				} else if(requestMethod.equals(GET_METHOD)) {
					// get values of given key request
					String key = clientRequest[1];
					List<String> values = dataStoreManager.get(key);
					return DONE_MESSAGE + SEPARATOR + values.toString();
				}
				
				return DONE_MESSAGE;
			} catch(Exception e) {
				return ERROR_MESSAGE;
			}
		}
		
	}

}
