import com.sun.net.httpserver.HttpServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Server
{
    public static final ConcurrentHashMap<String, String> dataMap = new ConcurrentHashMap<>();
    private static int port;
    private static String bindIP;
    private static Properties properties;

    private static void loadProperties(String file)
    {
        try
        {

            properties = new Properties();
            properties.load(new FileInputStream(new File(file)));
            System.out.println("Properties loaded successfully");
            System.out.println(properties);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.exit(0);
        }
    }


    public static void main(String[] args) throws Exception
    {
        if(args.length == 0)
        {
            System.out.println("Please load with server conf");
            System.exit(0);
        }

        loadProperties(args[0]);
        serverInit();
    }

    private static void serverInit() throws Exception
    {

        String master = null;
        int masterPort = -1;
        int replicationPort = -1;

        if(properties.containsKey(ServerConstants.BINDIP) && properties.containsKey(ServerConstants.PORT))
        {
            bindIP = properties.getProperty(ServerConstants.BINDIP);

            try
            {
                port = Integer.parseInt(properties.getProperty(ServerConstants.PORT));
            }
            catch(Exception e)
            {
                e.printStackTrace();
                System.exit(0);
            }
        }
        else
        {
            System.out.println("Invalid configuration for server");
            System.exit(0);
        }

        boolean isMaster = Boolean.parseBoolean(properties.getProperty(ServerConstants.IS_MASTER));

        if(!isMaster)
        {

            master = properties.getProperty(ServerConstants.MASTERIP);
            try
            {
                masterPort = Integer.parseInt(properties.getProperty(ServerConstants.MASTERPORT));
                replicationPort = Integer.parseInt(properties.getProperty(ServerConstants.REPLICATIONPORT));
            }
            catch(Exception e)
            {
                e.printStackTrace();
                System.exit(0);
            }

        }
        System.out.println("Init server as " + (isMaster ? "Master" : "Slave"));

        // server requests
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 100);
        httpServer.createContext("/get", httpExchange ->
        {
            String key = httpExchange.getRequestURI().getQuery();
            String response = String.valueOf(dataMap.get(key));

            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream out = httpExchange.getResponseBody();
            out.write(response.getBytes());
            out.close();

        });

        httpServer.createContext("/set", httpExchange ->
        {
            String key = httpExchange.getRequestURI().getQuery();
            String[] data = key.split("=");
            String response;
            if(data.length == 2)
            {
                response = dataMap.put(data[0], data[1]);
                if(response == null)
                {
                    response = "Inserted";
                }
                else
                {
                    response = "Updated";
                }
            }
            else
            {
                response = "Invalid arguments";
            }
            Replication.replication(Operation.SET, data[0], data[1]);
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream out = httpExchange.getResponseBody();
            out.write(response.getBytes());
            out.close();

        });


        httpServer.createContext("/del", httpExchange ->
        {
            String key = httpExchange.getRequestURI().getQuery();
            String response = dataMap.remove(key);
            if(response == null)
            {
                response = "Not exists";
            }
            else
            {
                response = "Deleted";
            }

            Replication.replication(Operation.DEL, key, null);
            httpExchange.sendResponseHeaders(200, response.length());
            OutputStream out = httpExchange.getResponseBody();
            out.write(response.getBytes());
            out.close();

        });

        if(isMaster)
        {
            httpServer.createContext("/replication", httpExchange ->
            {
                String query = httpExchange.getRequestURI().getQuery();
                String data[] = query.split(":");
                Replication.registerToMaster(data[0], Integer.parseInt(data[1]));
                String response = "success";
                httpExchange.sendResponseHeaders(200, response.length());
                OutputStream out = httpExchange.getResponseBody();
                out.write(response.getBytes());
                out.close();
            });
        }

        httpServer.start();

        System.out.println("Context created successfully");

        if(!isMaster)
        {
            startReplicationServer(master, masterPort, replicationPort);
        }
    }

    private static void startReplicationServer(String master, int masterPort, int replicationPort) throws Exception
    {
        String reg = "http://" + master + ":" + masterPort + "/replication?" + bindIP + ":" + replicationPort;

        URL url = new URL(reg);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        int code = connection.getResponseCode();
        if(code == 200)
        {
            System.out.println("Slave registered to master successfully");
        }
        else
        {
            System.out.println("Error while registering slave");
        }

        Replication.initReplicationServer(bindIP, replicationPort);
        System.out.println("Slave server started success");
    }

    enum Operation
    {
        GET, SET, DEL
    }

}
