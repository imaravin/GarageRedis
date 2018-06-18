import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

class Replication
{

    private static ServerSocket replicationServer;
    private static Socket client;
    private static final Set<Machine> clientsList = new HashSet<>();

    public static void initReplicationServer(int port) throws Exception
    {
        System.out.println("Initing replication server...");
        replicationServer = new ServerSocket(port, 10, InetAddress.getLocalHost());
        new Thread(() ->
        {
            try
            {
                acceptFromMaster();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }).start();
        System.out.println("Replication server started successfully");
    }

    private static void acceptFromMaster() throws Exception
    {

        while(true)
        {
            System.out.println("going to accept client");
            client = replicationServer.accept();
            System.out.println("Client connected @ " + client);

            try
            {
                InputStream inputStream = client.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String data = reader.readLine();
                process(data);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

        }
    }


    private static void sendToSlave(String data)
    {
        clientsList.forEach(machine ->
        {
            try
            {
                System.out.println("Machine details --- " + machine);
                client = new Socket(machine.address, machine.port);
                client.setTcpNoDelay(true);
                OutputStream os = client.getOutputStream();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(os));
                writer.println(data);
                writer.close();
                client.close();
                System.out.println("Data written to slave successfully");
            }
            catch(IOException e)
            {
                e.printStackTrace();
            }

        });

    }

    private static void process(String data)
    {
        String[] query = data.split(" ");
        if(Integer.parseInt(query[0]) == Server.Operation.SET.ordinal())
        {
            if(query.length == 3)
            {
                Server.dataMap.put(query[1], query[2]);
            }
            else
            {
                System.err.println("Invalid insert / update request");
            }
        }

        if(Integer.parseInt(query[0]) == Server.Operation.DEL.ordinal())
        {
            if(query.length == 2)
            {
                Server.dataMap.remove(query[1]);
            }
            else
            {
                System.err.println("Invalid delete request");
            }
        }

    }

    public static void registerToMaster(String clientAddress, int port)
    {
        Machine machine = new Machine(clientAddress, port);
        System.out.println("adding to list of slaves "+machine);
        clientsList.add(machine);
    }

    static void replication(Server.Operation operation, String key, String value)
    {
        // written to remote client using server
        // cascading the machines will be connected
        try
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append(operation.ordinal());
            buffer.append(" ");
            buffer.append(key);
            if(value!=null)
            {
                buffer.append(" ");
                buffer.append(value);
            }
            sendToSlave(buffer.toString());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
