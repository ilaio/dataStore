package com.dataStore.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class DataStoreClient {
	
	// port for client server communication
	private static final Integer PORT = 9090;
	
	// client params
	private Socket socket;
	private BufferedReader br;
	private PrintWriter pw;
	private Scanner sc = new Scanner(System.in);
	
	// some constants
	private final String EXIT_MESSAGE = "EXIT";
	private final String DONE_MESSAGE = "DONE";
	private final String SEPARATOR = "DATA_STORE_SEPARATOR";
	private final String COMMA = "DATA_STORE_COMMA";
	private final String GET_ALL_KEYS_METHOD = "getAllKeys";
	private final String RIGHT_ADD_METHOD = "rightAdd";
	private final String LEFT_ADD_METHOD = "leftAdd";
	private final String SET_METHOD = "set";
	private final String GET_METHOD = "get";

	public static void main(String[] args) {
		DataStoreClient client = new DataStoreClient();
		client.printWelcomeMessage();
		client.connectToServer();
		client.getClientRequests();
	}
	
	private void printWelcomeMessage() {
		System.out.println("Welcome to the Data Store program.");
	}
	
	// ask to provide server IP, and connect to server.
	private void connectToServer() {
		System.out.println("Enter IP Address of the Server:");
		String ip = sc.nextLine();
		try {
			socket = new Socket(ip, PORT);
			br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			pw = new PrintWriter(socket.getOutputStream(), true);
			
		} catch(Exception e) {
			System.out.println("Failed connection to server.");
			System.exit(1);
		}
	}
	
	// get requests from client and communicate to server
	private void getClientRequests() {
		while(true) {
			printRequestOptions();
			int requestNumber = sc.nextInt();
			switch(requestNumber) {
			case 1:
				handleGetAllKeysRequest();
				break;
			case 2:
				handleRightAddRequest();
				break;
			case 3:
				handleLeftAddRequest();
				break;
			case 4:
				handleSetRequest();
				break;
			case 5:
				handleGetRequest();
				break;
			case 6:
				handleExitRequest();
				break;
			}
		}
	}
	
	private void printRequestOptions() {
		System.out.println("\nEnter the number of one of the following requests:");
		System.out.println("1 - Get existing keys by given pattern");
		System.out.println("2 - Add value from the right to given key");
		System.out.println("3 - Add value from the left to given key");
		System.out.println("4 - Set values to given key");
		System.out.println("5 - Get values of given key");
		System.out.println("6 - Exit program");
	}
	
	// request pattern from client, send request to server, and display results
	private void handleGetAllKeysRequest() {
		System.out.println("Enter the requested pattern:");
		String pattern = getNonEmptyLine();
		pw.println(GET_ALL_KEYS_METHOD + SEPARATOR + pattern);
		handleServerResponse();
	}
	
	// request key and value from client, and request server to add value from the right
	private void handleRightAddRequest() {
		System.out.println("Enter the requested key:");
		String key = getNonEmptyLine();
		System.out.println("Enter the requested value:");
		String value = getNonEmptyLine();
		pw.println(RIGHT_ADD_METHOD + SEPARATOR + key + SEPARATOR + value);
		handleServerResponse();
	}
	
	// request key and value from client, and request server to add value from the left
	private void handleLeftAddRequest() {
		System.out.println("Enter the requested key:");
		String key = getNonEmptyLine();
		System.out.println("Enter the requested value:");
		String value = getNonEmptyLine();
		pw.println(LEFT_ADD_METHOD + SEPARATOR + key + SEPARATOR + value);
		handleServerResponse();
	}
	
	// request key and values from client, and send to server
	private void handleSetRequest() {
		System.out.println("Enter the requested key:");
		String key = getNonEmptyLine();
		System.out.println("Enter the requested values (enter empty line to continue):");
		String values = "";
		String value;
		while(!(value = sc.nextLine()).isEmpty()) {
			values += value + COMMA;
		}
		pw.println(SET_METHOD + SEPARATOR + key + SEPARATOR + values);
		handleServerResponse();
	}
	
	// request key from client, and get values from server
	private void handleGetRequest() {
		System.out.println("Enter the requested key:");
		String key = getNonEmptyLine();
		pw.println(GET_METHOD + SEPARATOR + key);
		handleServerResponse();
	}
	
	// send exit request to server, and close client
	private void handleExitRequest() {
		pw.println(EXIT_MESSAGE);
		try {
			socket.close();
		} catch(Exception e) {
			// exception while closing socket
		}
		System.exit(0);
	}
	
	// handle response from server
	private void handleServerResponse() {
		try {
			String response = br.readLine();
			if(response == null || response.equals("")) {
				System.exit(0);
			}
			String[] splitResponse = response.split(SEPARATOR);
			if(splitResponse[0].equals(DONE_MESSAGE)) {
				if(splitResponse.length == 1) {
					System.out.println("Request finished successfully");
				} else {
					System.out.println(splitResponse[1]);
				}
			} else {
				System.out.println("Request finished with errors");
			}
		} catch(Exception e) {
			// exception while reading response
		}
	}
	
	// scan line until a non-empty line is given
	private String getNonEmptyLine() {
		while(true) {
			String line = sc.nextLine();
			if(!line.isEmpty()) {
				return line;
			}
		}
	}

}
