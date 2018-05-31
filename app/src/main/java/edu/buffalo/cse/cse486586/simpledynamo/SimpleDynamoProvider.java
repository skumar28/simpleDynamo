package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.CancellationSignal;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	private static final String TAG = SimpleDynamoProvider.class.getName();
	String myPort;
	static final int SERVER_PORT = 10000;
	private static Node currentNode;
	private static String nodeIds[] = {"5554","5556","5558","5560","5562"};
	static final String DELIMETER = "##";
	static final String INSERT_DATA = "INSERT_DATA";
	static final String DELETE_DATA = "DELETE_DATA";
	static  final String QUERY_ALL_DATA = "QUERY_ALL_DATA";
	static final String RECOVER_DATA = "RECOVER_DATA";
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	static final String NODE_DEL = "//";

	public SimpleDynamoProvider() {
		super();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		try {
			String keyHash = genHash(selection);
			Log.e(TAG, "Delete: KeyHash ::" + keyHash +  "  for key: " +  selection);

			Node coordNode = findNode(keyHash, currentNode);
			Log.e(TAG, "MAIN DELETE: Coordinator node for key :" + keyHash +
					" is : " + coordNode.nodeId);
			String dataMessage = DELETE_DATA+DELIMETER+selection;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_DATA, dataMessage, coordNode, myPort);

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		Log.e(TAG, "MAIN INSERT: inside insert method");
		// We need to insert on proper place in ring
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		Log.e(TAG, "MAIN INSERT: Called for key:value = " + key + "::" + value);

		try {
			String keyHash = genHash(key);
			Log.e(TAG, "Find the coordNode for key: "+ keyHash);
			Node coordNode = findNode(keyHash, currentNode);
			Log.e(TAG, "MAIN INSERT: Coordinator node for key :" + keyHash + " is : " + coordNode.nodeId);

			String dataMessage = INSERT_DATA + DELIMETER + key + DELIMETER + value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_DATA, dataMessage, coordNode, myPort);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}


		return uri;

	}

	private Node findNode(String keyHash , Node tmpNode) {
		//Log.e(TAG, "Inside findNode Method tmpNode Port:" + tmpNode.port + " Id:" + tmpNode.port);
		if(tmpNode.nodeId.compareTo(tmpNode.prev.nodeId) < 0 && keyHash.compareTo(tmpNode.nodeId) > 0 &&
				keyHash.compareTo(tmpNode.prev.nodeId)>0) {
			//Log.e(TAG, "Inside findNode Method node found 1st if ");
			return tmpNode;
		}
		else if(tmpNode.nodeId.compareTo(tmpNode.prev.nodeId) < 0 && keyHash.compareTo(tmpNode.nodeId) < 0){
		//	Log.e(TAG, "Inside findNode Method node found 2nd if ");
			return  tmpNode;
		}
		else if((keyHash.compareTo(tmpNode.nodeId) < 0 && keyHash.compareTo(tmpNode.prev.nodeId) > 0)){
		//	Log.e(TAG, "Inside findNode Method node found 3rd if");
			return tmpNode;
		}
		else {
			//Log.e(TAG, "Inside findNode Method node node found (Recursioin)");
			return findNode(keyHash, tmpNode.next);
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String idStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String port = String.valueOf((Integer.parseInt(idStr) * 2));
		myPort = port;
		Log.e(TAG, "On Create :  create topology");
		currentNode = createTopologyInfo(myPort);
		Log.e(TAG, "Current Node : port: " +  currentNode.port + " ID:" + currentNode.nodeId);

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}
		String dataMessage = "OnCreateCallForRecovery";
		Log.e(TAG, "OnCreateCallForRecovery Initializing ClientTask");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, RECOVER_DATA, dataMessage, currentNode, myPort);

		return false;
	}
	// Store the key Value into this node fileSystem
	public void storeContent(String key, String value){
		//Log.e(TAG, "Main: Inside storeContent method");
		String filename = key;
		String string = value + "\n";
		FileOutputStream outputStream;
		//Log.v("fileName: ", filename + " file Content : " + string);

		try {
			outputStream =  getContext().openFileOutput(filename, Context.MODE_PRIVATE);
			outputStream.write(string.getBytes());
			outputStream.close();
		} catch (Exception e) {
			Log.e(TAG, "File write failed");
		}

		//Log.v("insert", key + " " + value);
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	private Node createTopologyInfo(String myNode) {
		Node tmpNode = null;
		try{
			// 5562 -> 5556 -> 5554 -> 5558 -> 5560
			Node node1 = new Node(genHash("5554"), "11108");
			Node node2 = new Node(genHash("5556"), "11112");
			Node node3 = new Node(genHash("5558"), "11116");
			Node node4 = new Node(genHash("5560"), "11120");
			Node node5 = new Node(genHash("5562"), "11124");

			node5.next = node2;
			node5.prev = node4;
			node2.next = node1;
			node2.prev = node5;
			node1.next= node3;
			node1.prev = node2;
			node3.next = node4;
			node3.prev = node1;
			node4.next = node5;
			node4.prev = node3;

			tmpNode = node1;

			while(true){
				if(tmpNode.port.equals(myPort)){
					return tmpNode;
				}
				else{
					tmpNode = tmpNode.next;
				}
			}

		}
		catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return  currentNode;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		//Log.e(TAG, "MAIN query: inside query method");
		Log.e(TAG, "MAIN query: Query for selection : " +  selection);
		String colNames[] = {"key","value"};
		MatrixCursor matrixCursor = new MatrixCursor(colNames);
		try {
			if(selection.equals("@")){
				List<String> allGlobalFile = getAllFiles();
				for(String file : allGlobalFile){
					String colVal[] = getSelectionValue(file);
					matrixCursor.addRow(colVal);
				}
			}
			else if(selection.equals("*")){
				// Query all data
				matrixCursor = getAllNodeData(currentNode);
			}
			else{
				String keyHash =  genHash(selection);
                Log.e(TAG, "MAIN Query: find the cordinator node for key : " +selection);
                Node coordNode = findNode(keyHash, currentNode);
                Log.e(TAG, "MAIN Query: Coordinator node for key :" + selection +
                        " is : " + coordNode.nodeId);

                String respNodesForKey = coordNode.nodeId + DELIMETER + coordNode.next.nodeId+ DELIMETER + coordNode.next.next.nodeId;
                String respNodesPort = coordNode.port + DELIMETER + coordNode.next.port + DELIMETER + coordNode.next.next.port;
                Log.e(TAG, "This Key: "+selection+ " Should be on Nodes:"+respNodesForKey+ " Port: "+ respNodesPort);
                if(respNodesForKey.contains(currentNode.nodeId)){
                    Log.e(TAG, "Key Should be replicated here return");
                    try {
                        String colVal[] = getSelectionValue(selection);
                        if(colVal == null){
							Log.e(TAG, "Key Should be replicated here But not found quering next replica");
							matrixCursor = getSingleKeyData(selection,coordNode.next);
						}
						else{
							matrixCursor.addRow(colVal);
						}
                    }
                    catch (Exception e){
                        Log.e(TAG,"getSelectionValue(selection)   Thrown Exception");
                        e.printStackTrace();
                    }

                }
                else{
                    // If the the node return null then query the replicas of the node
                    matrixCursor = getSingleKeyData(selection,coordNode);
                    if(matrixCursor == null){
                        Log.e(TAG, "MAIN Query: Coordinator's 1st replica node for key :" + selection +
                                " is : " + coordNode.next.nodeId);
                        matrixCursor = getSingleKeyData(selection,coordNode.next);
                    }
                    if(matrixCursor == null){
                        Log.e(TAG, "MAIN Query: Coordinator's 2nd replica node for key :" + selection +
                                " is : " + coordNode.next.next.nodeId);
                        matrixCursor = getSingleKeyData(selection,coordNode.next.next);
                    }
                }
			}
		}
		catch (NoSuchAlgorithmException e) {
			Log.e(TAG, "NoSuchAlgorithmException query method");
			e.printStackTrace();
		}
		Log.v("query ", selection + "Return data size : " + matrixCursor.getCount());
		return matrixCursor;
	}

	private MatrixCursor getSingleKeyData(String selection, Node coordNode) {
		String dataMessage = QUERY_ALL_DATA + DELIMETER + selection;
		Socket socket = null;
		String colNames[] = {"key","value"};
		MatrixCursor matrixCursor = new MatrixCursor(colNames);
		Log.e(TAG, "MAIN QUERY : query single key " + selection);
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(coordNode.port));

			socket.setSoTimeout(200);
			String queryMessage = dataMessage;
			// Write message to socket output stream
			PrintStream printStream = new PrintStream(socket.getOutputStream());
			printStream.println(queryMessage);
			Log.e(TAG, "MAIN QUERY : TASK : Sending message : " + queryMessage + " To port: " + coordNode.port);

			InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			//proposedSeqNo + FROM process + for msgID
			String queryResult = bufferedReader.readLine();
			Log.e(TAG, "MAIN QUERY : TASK : Received message : " + queryResult);
			if(queryResult != null && queryResult.length() > 0) {
				/// return result cursor from here
				matrixCursor = createMatrixCursor(matrixCursor, queryResult);
				Log.e(TAG, "MAIN QUERY : TASK : Returning matrix cursor with count : " + matrixCursor.getCount());
			}
			else{
			    return null;
            }

		} catch (UnknownHostException e) {
			Log.e(TAG, " getSingleKeyData socket UnknownHostException ");
		}
		catch (SocketTimeoutException e){
			Log.e(TAG, " getSingleKeyData socket SocketTimeoutException ");
			// If node is down then this exception might come
			return null;
		}
		catch (IOException e){
			// If node is down then this exception might come
			Log.e(TAG, " getSingleKeyData socket IOException ");
			return null;
		}
		return matrixCursor;
	}

	private MatrixCursor getAllNodeData(Node curNode) {
		boolean isDone = false;
		Node tmpNode = curNode.next;
		String colNames[] = {"key","value"};
		MatrixCursor matrixCursor = new MatrixCursor(colNames);
		String dataMessage = QUERY_ALL_DATA + DELIMETER + "@";
		try {
			while(!isDone){
				Log.e(TAG, "MAIN QUERY : TASK Inside While loop");
				if(tmpNode.equals(curNode)){
					isDone = true;
					Log.e(TAG, "While loop Complete after this iterations: nextPort: " +
							tmpNode.port + " MyPort:" + myPort);
				}

				String queryResult = queryData(dataMessage, tmpNode);

				if(queryResult != null && queryResult.length() > 0) {
					/// return result cursor from here
					matrixCursor = createMatrixCursor(matrixCursor, queryResult);
					Log.e(TAG, "MAIN QUERY [@]: TASK : Returning matrix cursor with count : " + matrixCursor.getCount());
				}
				else{
					Log.e(TAG, "MAIN QUERY [@]: TASK : Received message is NULL " + queryResult);
				}

				tmpNode = tmpNode.next;
			}
		}
		catch (Exception e) {
			Log.e(TAG, "ClientTask Exception in getAllNodeData");
		}
		/*catch (Exception e){
			Log.e(TAG, "ClientTask socket Exception");
		}*/

		return matrixCursor;
	}

	private String queryData(String dataMessage, Node tmpNode){
	    String queryResult = null;
	    try{
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(tmpNode.port));
            socket.setSoTimeout(200);
            String queryMessage = dataMessage;
            // Write message to socket output stream
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(queryMessage);
            Log.e(TAG, "MAIN QUERY [@]: TASK : Sending message : " + queryMessage + " To port: " + tmpNode.port);

            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            //proposedSeqNo + FROM process + for msgID
            queryResult = bufferedReader.readLine();
            //Log.e(TAG, "MAIN QUERY [@]: TASK : Received message : " + queryResult + " From port " + tmpNode.port);
        }
        catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException queryData");
            return null;
        }catch (SocketTimeoutException e){
            Log.e(TAG, "ClientTask socket SocketTimeoutException queryData");
            return null;
        }
        catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException queryData");
            return null;
        }

    return queryResult;
    }
	private MatrixCursor createMatrixCursor(MatrixCursor matrixCursor, String queryResult) {
		String[] resultToken = queryResult.split(DELIMETER);
		//Log.e(TAG, "MAIN QUERY Inside createMatrixCursor method and no of messg : = " + resultToken.length);
		for(int i =0 ; i < resultToken.length; i++){
			String message = resultToken[i];
			String colVal[] = {message.split(NODE_DEL)[0],message.split(NODE_DEL)[1]};
			matrixCursor.addRow(colVal);
		}
		return matrixCursor;
	}

	private String[] getSelectionValue(String selection){
		try {
			FileInputStream fileInputStream = getContext().openFileInput(selection);
			InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String line = bufferedReader.readLine();
			//Log.v("Line Read from file", line);
			String colVal[] = {selection, line};
			return colVal;
		}
		catch (FileNotFoundException e){
			Log.e(TAG, "FileNotFoundException for key :" + selection);
			return null;
		}
		catch (IOException e){
			Log.e(TAG, "IOException for key :" + selection);
			return null;
		}
	}

	List<String> getAllFiles(){
		List<String> allFiles = new ArrayList<String>();
		File folder = new File(String.valueOf(getContext().getFilesDir()));
		for (final File fileEntry : folder.listFiles()) {
			allFiles.add(fileEntry.getName());
		}

		return allFiles;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

	private class ServerTask  extends AsyncTask<ServerSocket, String, Void>{
		@Override
		protected void finalize() throws Throwable {
			super.finalize();
		}

		@Override
		protected Void doInBackground(ServerSocket... serverSockets) {
			ServerSocket serverSocket = serverSockets[0];
			while (true) {
				try {
					Log.e(TAG, "Ready to accept Connection");
					Socket socket = serverSocket.accept();

					InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

					String message = bufferedReader.readLine();
					if(message != null){
                        Log.e(TAG, "SERVER : Received Message : " + message);

                        String msgToken [] = message.split(DELIMETER);

                        if(msgToken[0].equals(INSERT_DATA)){
                            String key = msgToken[1];
                            String value = msgToken[2];
                            Log.e(TAG, "MAIN INSERT: SERVER: Insert Called from server task with Key:Value = " + key + ":" + value);
                            storeContent(key,value);
                        }

                        if(msgToken[0].equals(QUERY_ALL_DATA)){
                            //String dataMessage = QUERY_ALL_DATA + DELEMETER + "@" ;
                            String selKey = msgToken[1];
                            Log.e(TAG, "MAIN QUERY : SERVER [QUERY_ALL_DATA] condition: with selKey : " + selKey);
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                            Cursor resultCursor = query(mUri, null, selKey, null, null);
                            if (resultCursor == null) {
                                Log.e(TAG, "MAIN QUERY : SERVER [QUERY_ALL_DATA]: Result resultCursor null");
                            }
                            else{
                                String cursorMessages = getMessageFromCursor(resultCursor);

                                PrintStream printStream = new PrintStream(socket.getOutputStream());
                                printStream.println(cursorMessages);
                                Log.e(TAG, "MAIN QUERY : SERVER [QUERY_ALL_DATA]: Sending query result to client: ");
                            }
                        }
                        if (msgToken[0].equals(DELETE_DATA)){
                            String selection = msgToken[1];
                            Log.e(TAG, "MAIN DELETE : SERVER [DELETE_DATA] condition: with selKey : " + selection);
                            getContext().deleteFile(selection);
                        }
                    }
					else{
                        Log.e(TAG, "SERVER : Received Message : null" );
                    }

					socket.close();
					Log.e(TAG, "socket closed");
				} catch (IOException e) {
					Log.e(TAG,"SERVER: IOException");
				}
			}
		}

		private String getMessageFromCursor(Cursor resultCursor){
			StringBuilder sb = new StringBuilder();
			while(resultCursor.moveToNext()){
				int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
				int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
				String returnKey = resultCursor.getString(keyIndex);
				String returnValue = resultCursor.getString(valueIndex);
				sb.append(returnKey+NODE_DEL+returnValue).append(DELIMETER);
				//Log.e(TAG, "SERVER: query messge is : " + returnKey + "//" + returnValue);
			}
			if(sb.length() >2){
				return sb.toString().substring(0, sb.length()-2);
			}
			else{
				Log.e(TAG, "method getMessageFromCursor empty cursor");
				return "";
			}
		}

		private Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}
	}


	private class ClientTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... msgs) {
			//AsyncTask.SERIAL_EXECUTOR, INSERT_DATA, dataMessage, coordNode, myPort
			//String dataMessage = INSERT_DATA + DELIMETER + key + DELIMETER + value;
			try{
				String msgType = (String) msgs[0];
				String dataMsg = (String) msgs[1];
				Node cordNode = (Node) msgs[2];
				String curNodePort = (String) msgs[3];

				Log.e(TAG,"CLIENT TASK: msgType: " + msgType + " dataMessage: " + dataMsg +
						" CoordinatorNode : "+ cordNode.port + " Current port: " + curNodePort);

				if(msgType.equals(INSERT_DATA)){
					// Send data to coordinator and two replicas
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(cordNode.port));
					PrintStream printStream = new PrintStream(socket.getOutputStream());
					printStream.println(dataMsg);
					Log.e(TAG, "CLIENT TASK: Sending insert message : "+ dataMsg + " To port: " + cordNode.port);

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(cordNode.next.port));
					printStream = new PrintStream(socket.getOutputStream());
					printStream.println(dataMsg);
					Log.e(TAG, "CLIENT TASK: Sending insert message : "+ dataMsg + " To " +
							"port: " + cordNode.next.port);

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(cordNode.next.next.port));
					printStream = new PrintStream(socket.getOutputStream());
					printStream.println(dataMsg);
					Log.e(TAG, "CLIENT TASK: Sending insert message : "+ dataMsg + " To " +
							"port: " + cordNode.next.next.port);
				}
				if(msgType.equals(DELETE_DATA)){
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(cordNode.port));
					PrintStream printStream = new PrintStream(socket.getOutputStream());
					printStream.println(dataMsg);
					Log.e(TAG, "CLIENT TASK: Sending delete message : "+ dataMsg + " To port: " + cordNode.port);

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(cordNode.next.port));
					printStream = new PrintStream(socket.getOutputStream());
					printStream.println(dataMsg);
					Log.e(TAG, "CLIENT TASK: Sending delete message : "+ dataMsg + " To " +
							"port: " + cordNode.next.port);

					socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(cordNode.next.next.port));
					printStream = new PrintStream(socket.getOutputStream());
					printStream.println(dataMsg);
					Log.e(TAG, "CLIENT TASK: Sending delete message : "+ dataMsg + " To " +
							"port: " + cordNode.next.next.port);
				}
				if(msgType.equals(RECOVER_DATA)){
					//Log.e(TAG, "CLIENT TASK : Inside recovery dataMessage: " + dataMsg);
                    //long startTime = System.currentTimeMillis();
					Log.e(TAG, "CLIENT TASK : Inside recovery currentNode: " + cordNode.port);
					Node curNode = cordNode;
					//Delete all the local existing files
					/*List<String> allFiles = getAllFiles();
					for(String file : allFiles){
						getContext().deleteFile(file);
					}*/
					//
					String colNames[] = {"key","value"};
					MatrixCursor matrixCursor = new MatrixCursor(colNames);
					Cursor resultCursor = getNextAndPrevNodeData(matrixCursor, curNode);
					/*Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
					//Log.e(TAG, "CLIENT TASK : Inside recovery getting data from all the node");
					Cursor resultCursor = query(mUri, null, "*", null, null);*/
					if(resultCursor.getCount() > 0){
                        Log.e(TAG, "CLIENT TASK : Inside recovery  resultCursor size  " + resultCursor.getCount());
                        // This node is responsible for the key of
                        // 1. this node
                        // 2. Previous Node
                        // 3. Previous to previous Node
                        String respForNodes = curNode.nodeId + DELIMETER + curNode.prev.nodeId+ DELIMETER + curNode.prev.prev.nodeId;
                        String respForNodesPorts = curNode.port + DELIMETER + curNode.prev.port + DELIMETER + curNode.prev.prev.port;
                        //Log.e(TAG, "CLIENT TASK : Inside recovery This Node is responsible for " +
                        //        "keys of: " + respForNodes + " Port Nos: " +  respForNodesPorts);
                        int count = 0;
                        while(resultCursor.moveToNext()){
                            int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                            int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                            String key = resultCursor.getString(keyIndex);
                            String value = resultCursor.getString(valueIndex);

                            String keyHash = genHash(key);

                            Node coordinatorNode = findNode(keyHash,curNode);

                            if(respForNodes.contains(coordinatorNode.nodeId)){
                                storeContent(key,value);
                                count++;
                            }
                        }
                        Log.e(TAG, "CLIENT TASK : Inside recovery Method ends : " +
                                "Total message saved : " + count);
                       // long stopTime = System.currentTimeMillis();
                        //long elapsedTime = stopTime - startTime;
                        //Log.e(TAG, "CLIENT TASK : Inside recovery Method : Time Taken (ms) " + elapsedTime);
                    }
                    else{
					    Log.e(TAG,"CLIENT TASK : Inside recovery Method  resultCursor size is zero");
                    }
				}
			}
		 	catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			}
			catch(NoSuchAlgorithmException e){
				Log.e(TAG, "ClientTask socket NoSuchAlgorithmException");
			}

			return null;
		}

		private Cursor getNextAndPrevNodeData(MatrixCursor matrixCursor, Node curNode) {
			String dataMessage = QUERY_ALL_DATA + DELIMETER + "@";

			String queryResult = queryData(dataMessage, curNode.prev);

			if(queryResult != null && queryResult.length() > 0) {
				/// return result cursor from here
				matrixCursor = createMatrixCursor(matrixCursor, queryResult);
				Log.e(TAG, "MAIN QUERY [@]: TASK : getNextAndPrevNodeData (Prev) Returning matrix cursor with count : " + matrixCursor.getCount());
			}
			else{
				// Try one more previous node
				queryResult = queryData(dataMessage, curNode.prev.prev);
				if(queryResult != null && queryResult.length() > 0){
					matrixCursor = createMatrixCursor(matrixCursor, queryResult);
				}else{
					Log.e(TAG, "MAIN QUERY [@]: TASK : getNextAndPrevNodeData (else PrevPrev) Received message is NULL " + queryResult);
				}
			}
			queryResult = queryData(dataMessage, curNode.next);
			if(queryResult != null && queryResult.length() > 0) {
				/// return result cursor from here
				matrixCursor = createMatrixCursor(matrixCursor, queryResult);
				Log.e(TAG, "MAIN QUERY [@]: TASK : getNextAndPrevNodeData (next)Returning matrix cursor with count : " + matrixCursor.getCount());
			}
			else{
				// try one more next node
				queryResult = queryData(dataMessage, curNode.next.next);
				if(queryResult != null && queryResult.length() > 0){
					matrixCursor = createMatrixCursor(matrixCursor, queryResult);
				}else{
					Log.e(TAG, "MAIN QUERY [@]: TASK : getNextAndPrevNodeData (else PrevPrev) Received message is NULL " + queryResult);
				}
			}

		return matrixCursor;
		}
	}
}
