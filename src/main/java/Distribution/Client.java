package Distribution;

import Authentication.Constants;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client {

    private String clientId;
    private Socket socket;
    private BufferedWriter writer;
    private BufferedReader reader;

    public Client(Socket socket, String username) {
        try {
            this.socket = socket;
            this.clientId = username;
            this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        } catch (IOException e) {
            closeEverything(socket, reader, writer);
        }
    }

    public void sendMessage(String filePath) {
        try {
            File f = new File(filePath);
            if (!f.exists()) {
                System.out.println("InValid File Path");
                return;
            }
            String data = clientId + Constants.distributedDelimiter + filePath + Constants.distributedDelimiter;
            Scanner sc = new Scanner(new FileReader(f.getAbsolutePath()));

            while (sc.hasNext()) {
                data += sc.nextLine() + Constants.distributedDelimiter;
            }

            writer.write(data);
            writer.newLine();
            writer.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listenForMessage() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String msgFromServer;
                while (socket.isConnected()) {
                    try {
                        msgFromServer = reader.readLine();
                        processMessage(msgFromServer);
                    } catch (IOException e) {
                        closeEverything(socket, reader, writer);
                    }
                }
            }
        }).start();
    }

    public void processMessage(String data) throws IOException {
        if(data == null){
            return;
        }
        if(data.trim().length() <= 0){
            return;
        }
        String dataArr[] = data.split(Constants.distributedDelimiter);
        File f = null;
        String finalData = "";
        for (int i = 0; i < dataArr.length; i++) {
            if (i == 0) {
            } else if (i == 1) {
                f = new File(dataArr[i]);
                if (!f.exists()) {
                    f.createNewFile();
                }
                new FileWriter(f.getAbsolutePath(), false).close();
            } else {
                finalData += dataArr[i] + "\n";
            }
        }
        if (finalData.length() > 0) {
            try {
                FileWriter w = new FileWriter(f.getAbsolutePath());
                w.write(finalData);
                w.flush();
                w.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void closeEverything(Socket socket, BufferedReader bufferedReader, BufferedWriter bufferedWriter) {
        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
