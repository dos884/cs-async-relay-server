using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;

public enum MessageType : byte
{
    NONE = 0,
    GAME_ENDED = 1,
    GAME_START = 2,
    YOUR_TURN = 3,
    KEEP_ALIVE = 4,
    UDP_USERNAME_CONFIRM = 5,
    DISCONNECT = 6,
    YOU_LOSE = 7,
    YOU_WIN = 8
}

public class Logger
{
    private static LinkedList<string> _messages;
    private static string _fileName;
    private static string _exePath;
    private static Thread _writerThread;

    public static void Init()
    {
        _exePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        _fileName = "log-" + DateTime.Now.ToString("dd-MM-yyyy _ HH-mm-ss") + ".txt";
        Console.WriteLine(_fileName);
        _fileName = Path.Combine(_exePath, _fileName);
        Console.WriteLine(_fileName);
        _messages = new LinkedList<string>();
        _writerThread = new Thread(DumpAllToFile);
        _writerThread.Start();
    }

    public static void Log(string msg)
    {
        Console.WriteLine(msg);
        lock (_messages)
        {
            _messages.AddLast(msg);
        }
        
    }

    private static void DumpAllToFile()
    {
        while (true)
        {
            lock (_messages)
            {
                var it = _messages.First;
                for (int i = 0; i < _messages.Count; i++)
                {
                    using (StreamWriter w = File.AppendText(_fileName))
                    {
                        DumpSingleToFile(it.Value, w);
                    }

                    it = it.Next;
                    _messages.RemoveFirst();
                }
            }

            Thread.Sleep(100);
        }
    }

    private static void DumpSingleToFile(string msg, TextWriter w)
    {
        w.WriteLine("[" + DateTime.Now.ToString("HH:mm:ss.fff") + "]");
        w.WriteLine(msg);
        w.WriteLine("--------------------------");
    }
}

public class WholeClient
{
    public IPEndPoint udpEndpoint;
    public string username;
    public Socket relatedTcpClient;
    public bool udpLoginConfirmed = false;
    public bool isTcpReadingInProgress = false;

    public WholeClient(string username, IPEndPoint udpEndpoint, Socket relatedTcpClient)
    {
        this.username = username;
        this.udpEndpoint = udpEndpoint;
        this.relatedTcpClient = relatedTcpClient;
    }
}

public class UdpLoginInfo
{
    public byte[] data;
    public IPEndPoint ipEndPoint;

    public UdpLoginInfo(byte[] data, IPEndPoint ipEndPoint)
    {
        this.data = data;
        this.ipEndPoint = ipEndPoint;
    }
}

// State object for reading client data asynchronously  
public class TcpStateObject
{
    // Size of receive buffer.  
    public const int BufferSize = 1024;

    // Receive buffer.  
    public byte[] buffer = new byte[BufferSize];

    // Received data string.
    public StringBuilder sb = new StringBuilder();

    // Client socket.
    public Socket workSocket = null;

    // The time a peer started connection
    public DateTime timeConnected;

    //
    public string username;
}

public class UdpState
{
    public UdpClient client;
    public IPEndPoint ep;
}

public class AsynchronousServer
{
    private static bool isConnectionInProgress = false;
    private static bool isUdpReadInProgress = false;
    private const int SERVER_PORT_TCP = 8888;
    private const int SERVER_PORT_UDP = 9990;
    private const int MAX_RELAYED_USERS = 10000;
    private const int MAX_TIME_BETWEEN_KEEPALIVES = 30000;
    public const string EOF = "<EOF>", SEPARATOR = "~";
    private const string SERVER_PASS = "alpha_centauri_2077";
    private static Socket _tcpListener;
    private static UdpClient _udpClient;
    static ConcurrentDictionary<string, WholeClient> wholeClientsMapping; //username->wc
    static ConcurrentDictionary<string, string> clientMapping; // username->username
    static ConcurrentDictionary<string, string> udpEpToUsernameMapping;
    static ConcurrentDictionary<string, DateTime> keepaliveTimerMapping;
    private static ConcurrentDictionary<string, byte> unmonitoredTcpUsers; // SET
    private static ConcurrentDictionary<string,byte> udpClientEndpointsSet; //  SET
    private static ConcurrentDictionary<string,byte> unconfirmedUdps; //SET
    private static ConcurrentDictionary<string,byte> relayedClients; // SET
    private static AsyncCallback _tcpReadAsyncCallback;
    private static Thread _tcpListenThread = new Thread(TcpListenDelegate);

    public static void InitTcpListener()
    {
        wholeClientsMapping = new ConcurrentDictionary<string, WholeClient>();
        clientMapping = new ConcurrentDictionary<string, string>();
        udpEpToUsernameMapping = new ConcurrentDictionary<string, string>();
        keepaliveTimerMapping = new ConcurrentDictionary<string, DateTime>();
        unmonitoredTcpUsers = new ConcurrentDictionary<string,byte>();
        udpClientEndpointsSet = new ConcurrentDictionary<string, byte>();
        unconfirmedUdps = new ConcurrentDictionary<string, byte>();
        relayedClients = new ConcurrentDictionary<string, byte>();
        _tcpReadAsyncCallback = new AsyncCallback(TcpReadCallback);
        // Establish the local endpoint for the socket.  
        // The DNS name of the computer  
        // running the listener is "host.contoso.com".  
        IPHostEntry ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
        IPAddress ipAddress = ipHostInfo.AddressList[0];
        ipAddress = IPAddress.Any;
        Logger.Log(ipAddress.ToString());
        IPEndPoint localEndPoint = new IPEndPoint(ipAddress, SERVER_PORT_TCP);

        // Create a TCP/IP socket.  
        _tcpListener = new Socket(ipAddress.AddressFamily,
            SocketType.Stream, ProtocolType.Tcp);

        // Bind the socket to the local endpoint and listen for incoming connections.  
        try
        {
            _tcpListener.Bind(localEndPoint);
            _tcpListener.Listen(10000);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    public static void Listen()
    {
        if (!isConnectionInProgress)
        {
            // Wait until a connection is made before continuing.
            isConnectionInProgress = true;
            // Start an asynchronous socket to listen for connections.  
            Logger.Log("Waiting for a connection...");
            _tcpListener.BeginAccept(
                new AsyncCallback(AcceptCallback),
                _tcpListener);
        }
    }

    private static int numAcceptCallbackCalled = 0;
    public static void AcceptCallback(IAsyncResult ar)
    {
        // Signal the main thread to continue.  
        isConnectionInProgress = false;
        numAcceptCallbackCalled++;
        // Get the socket that handles the client request.  
        Socket listener = (Socket) ar.AsyncState;
        Socket handler = null;
        try
        {
            handler = listener.EndAccept(ar);
            Logger.Log(string.Format("============= TCP connected {0}, called: {1}", 
                handler.RemoteEndPoint, numAcceptCallbackCalled));
            // Create the state object.  
            TcpStateObject tcpState = new TcpStateObject();
            tcpState.workSocket = handler;
            handler.BeginReceive(tcpState.buffer, 0, TcpStateObject.BufferSize, 0,
                new AsyncCallback(InitialReadCallback), tcpState);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    private static void TcpListenDelegate()
    {
        while (true)
        {
            var handler = _tcpListener.Accept();
            numAcceptCallbackCalled++;
            Logger.Log(string.Format("============= TCP connected {0}, called: {1}", 
                handler.RemoteEndPoint, numAcceptCallbackCalled));
            // Create the state object.  
            TcpStateObject tcpState = new TcpStateObject();
            tcpState.workSocket = handler;
            handler.BeginReceive(tcpState.buffer, 0, TcpStateObject.BufferSize, 0,
                new AsyncCallback(InitialReadCallback), tcpState);
            
        }
    }

    private static int numInitialReadCalled = 0;
    public static void InitialReadCallback(IAsyncResult ar)
    {
        numInitialReadCalled++;
        String content = String.Empty;

        // Retrieve the state object and the handler socket  
        // from the asynchronous state object.  
        TcpStateObject tcpState = (TcpStateObject) ar.AsyncState;
        Socket handler = tcpState.workSocket;

        // Read data from the client socket.
        int bytesRead = 0;
        try
        {
            bytesRead = handler.EndReceive(ar);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
            return;
        }

        if (bytesRead > 0)
        {
            try
            {
                // There  might be more data, so store the data received so far.  
                tcpState.sb.Append(Encoding.UTF8.GetString(
                    tcpState.buffer, 0, bytesRead));
            }
            catch (Exception e)
            {
                Logger.Log(e.ToString());
                return;
            }

            // Check for end-of-file tag. If it is not there, read
            // more data.  
            content = tcpState.sb.ToString();
            if (content.IndexOf(EOF) > -1)
            {
                // All the data has been read from the
                // client. Display it on the console.  
                Logger.Log(string.Format("Read {0} bytes from socket. \nData : {1} \niRead_called {2} wc_count{3}",
                    content.Length, content, numInitialReadCalled,wholeClientsMapping.Count));
                // ignore EOF:
                content = content.Split(EOF)[0];
                string[] descriptors = content.Split(SEPARATOR);
                string username;
                if (descriptors.Length==2 && descriptors[0]==SERVER_PASS)
                {
                    username = descriptors[1];
                }
                else
                {
                    return;
                }
                keepaliveTimerMapping[username] = DateTime.Now;
                if (wholeClientsMapping.ContainsKey(username))
                {
                    wholeClientsMapping[username].relatedTcpClient = handler;
                }
                else
                {
                    wholeClientsMapping[username] = new WholeClient(username, null, handler);
                }

                Logger.Log(string.Format("binded tcp ip{0} to username {1}", handler.RemoteEndPoint, username));
                clientMapping.TryAdd(username, null);
                if (username==null)
                {
                    Logger.Log("how is this null");
                }
                unmonitoredTcpUsers.TryAdd(username,0);
                // Echo the data back to the client.  
                //SendTcp(handler, content);
            }
            else
            {
                try
                {
                    // Not all data received. Get more.  
                    handler.BeginReceive(tcpState.buffer, 0, TcpStateObject.BufferSize, 0,
                        new AsyncCallback(InitialReadCallback), tcpState);
                }
                catch (Exception e)
                {
                    Logger.Log(e.ToString());
                }
            }
        }

        //todo disable this print - dont want to try catch here!
        Logger.Log("returning from InitialReadCallback: " + handler?.RemoteEndPoint);
    }

    private static void SendTcp(Socket handler, byte msg)
    {
        byte[] bytes = new[] {msg};
        try
        {
            handler.BeginSend(bytes, 0, 1, 0,
                new AsyncCallback(SendCallback), handler);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    private static void SendTcp(Socket handler, String data)
    {
        // Convert the string data to byte data using ASCII encoding.  
        byte[] byteData = Encoding.UTF8.GetBytes(data);
        try
        {
            // Begin sending the data to the remote device.  
            handler.BeginSend(byteData, 0, byteData.Length, 0,
                new AsyncCallback(SendCallback), handler);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    private static void SendCallback(IAsyncResult ar)
    {
        try
        {
            // Retrieve the socket from the state object.  
            Socket handler = (Socket) ar.AsyncState;

            // Complete sending the data to the remote device.  
            int bytesSent = 0;

            bytesSent = handler.EndSend(ar);

            Logger.Log("TCP Sent: " + bytesSent + " bytes to client: " + handler?.RemoteEndPoint);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    private static void BulkTcpRead()
    {
        List<string> keys;
        try
        {
            keys = new List<string>(unmonitoredTcpUsers.Keys);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
            return;
        }
        foreach(var u in keys)
        {
            WholeClient wc=null;
            try
            {
                wc = wholeClientsMapping[u];
            }
            catch (Exception e)
            {
                Logger.Log(e.ToString());
                continue;
            }
            unmonitoredTcpUsers.Remove(u, out byte b);
            if (!wc.isTcpReadingInProgress)
            {
                Socket handler = wc.relatedTcpClient;
                // Create the state object.  
                TcpStateObject tcpState = new TcpStateObject();
                tcpState.workSocket = handler;
                tcpState.username = u;
                try
                {
                    handler.BeginReceive(tcpState.buffer, 0, TcpStateObject.BufferSize, 0,
                        new AsyncCallback(TcpReadCallback), tcpState);
                    wc.isTcpReadingInProgress = true;
                    
                }
                catch (Exception e)
                {
                    Logger.Log(e.ToString());
                    DisconnectClient(u);
                    
                }
            }
        }
    }

    public static void TcpReadCallback(IAsyncResult ar)
    {
        int bytesRead = 0;
        TcpStateObject tcpState = null;
        Socket handler = null;
        try
        {
            // Retrieve the state object and the handler socket  
            // from the asynchronous state object.  
            tcpState = (TcpStateObject) ar.AsyncState;
            handler = tcpState.workSocket;
            //wholeClientsMapping[tcpState.username].isTcpReadingInProgress = false;
            // Read data from the client socket.
            bytesRead = handler.EndReceive(ar);
        }
        catch (Exception e)
        {
            Logger.Log("At TcpReadCallback " + e);
            DisconnectClient(tcpState?.username);
            return;
        }

        try
        {
            if (bytesRead > 0)
            {
                //Console.WriteLine("TCP read {0} bytes",bytesRead);
                for (int i = 0; i < bytesRead; i++)
                {
                    if (tcpState.buffer[i] == (byte) MessageType.KEEP_ALIVE)
                    {
                        keepaliveTimerMapping[tcpState.username] = DateTime.Now;
                    }
                    else if (tcpState.buffer[i] == (byte) MessageType.UDP_USERNAME_CONFIRM)
                    {
                        wholeClientsMapping[tcpState.username].udpLoginConfirmed = true;
                        Logger.Log(string.Format("Got (UDP) {0} | {1}  ACK for login",
                            wholeClientsMapping[tcpState.username].udpEndpoint, tcpState.username));
                    }
                    else if (tcpState.buffer[i] == (byte) MessageType.DISCONNECT)
                    {
                        DisconnectClient(tcpState.username);
                    }
                    else if (clientMapping[tcpState.username] != null)
                    {
                        Socket otherClient = wholeClientsMapping[clientMapping[tcpState.username]].relatedTcpClient;
                        SendTcp(otherClient, tcpState.buffer[i]);
                    }
                }
            }
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }

        try
        {
            handler?.BeginReceive(tcpState.buffer, 0, TcpStateObject.BufferSize, 0,
                _tcpReadAsyncCallback, tcpState);
        }
        catch (ObjectDisposedException e)
        {
            Logger.Log("System.ObjectDisposedException - this is normal");
        }
        catch (Exception u)
        {
            Logger.Log(u.ToString());
        }
    }

    public static void DisconnectClient(string username)
    {
        try
        {
            if (!wholeClientsMapping.ContainsKey(username))
            {
                return;
            }

            Logger.Log(string.Format("XXXXXXX closing for username {0}, ip: {1}",
                username, wholeClientsMapping[username].relatedTcpClient?.RemoteEndPoint));
            relayedClients.TryRemove(username,out byte b);
            if (clientMapping[username] != null)
            {
                // client is relayed - notify other side
                string relayedUser = clientMapping[username];
                Logger.Log(string.Format("Disconnected user {0} was relayed to {1}", username, relayedUser));
                relayedClients.TryRemove(relayedUser,out b);
                clientMapping[relayedUser] = null;
                SendTcp(wholeClientsMapping[relayedUser].relatedTcpClient, (byte) MessageType.GAME_ENDED);
            }

            // remove username from all the data structures
            string value;
            clientMapping.TryRemove(username,out value);
            //todo should be try catch
            if (wholeClientsMapping[username].udpEndpoint != null)
            {
                udpClientEndpointsSet.TryRemove(wholeClientsMapping[username].udpEndpoint.ToString(),out b);
                udpEpToUsernameMapping.TryRemove(wholeClientsMapping[username].udpEndpoint.ToString(),out value);
            }
            else
            {
                Logger.Log("at DisconnectClient: client did not udp connect");
            }

            keepaliveTimerMapping.TryRemove(username,out DateTime d);
            unmonitoredTcpUsers.TryRemove(username,out b);
            unconfirmedUdps.TryRemove(username, out b);
            //close socket
            wholeClientsMapping[username].relatedTcpClient?.Shutdown(SocketShutdown.Both);
            wholeClientsMapping[username].relatedTcpClient?.Close();
            // finally remove client from the wholeclientmap
            wholeClientsMapping.TryRemove(username,out WholeClient w);
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    public static void InitUdp()
    {
        IPEndPoint endPoint = new IPEndPoint(IPAddress.Any, SERVER_PORT_UDP);
        _udpClient = new UdpClient(endPoint);
        Logger.Log("current udp queue "+_udpClient.Client.ReceiveBufferSize);
        _udpClient.Client.ReceiveBufferSize = 524288;
        Logger.Log("increased udp queue "+_udpClient.Client.ReceiveBufferSize);
    }

    public static void UdpReceiveCallback(IAsyncResult ar)
    {
        //todo should be guarded by a mutex or try-catch
        IPEndPoint e = new IPEndPoint(IPAddress.Any, SERVER_PORT_UDP);
        byte[] data = _udpClient.EndReceive(ar, ref e);
        //isUdpReadInProgress = false;
        //Console.WriteLine("UDP read {0} bytes from {1}",data.Length,e);
        if (!udpClientEndpointsSet.ContainsKey(e.ToString()))
        {
            UdpLogin(e, data);
        }
        else if (data.Length > 1) // make sure its not a keep alive packet
        {
            if (udpEpToUsernameMapping.ContainsKey(e.ToString()))
            {
                try
                {
                    //error prone
                    var recUsername = udpEpToUsernameMapping[e.ToString()];
                    if (wholeClientsMapping.ContainsKey(recUsername))
                    {
                        WholeClient recWc = wholeClientsMapping[recUsername];
                        if (recWc.udpLoginConfirmed && clientMapping.ContainsKey(recUsername)
                                                    && clientMapping[recUsername] != null)
                        {
                            string mappedUsername = clientMapping[recUsername];
                            if (wholeClientsMapping[mappedUsername].udpLoginConfirmed)
                            {
                                IPEndPoint mappedEp = wholeClientsMapping[mappedUsername].udpEndpoint;
                                _udpClient.Send(data, data.Length, mappedEp);
                            }
                        }
                    }
                }
                catch (Exception exception)
                {
                    Logger.Log(exception.ToString());
                }
            }
        }

        _udpClient.BeginReceive(new AsyncCallback(UdpReceiveCallback), null);
    }

    private static int numUdpLoginCalls = 0;
    public static void UdpLogin(IPEndPoint ep, byte[] data)
    {
        numUdpLoginCalls++;
        udpClientEndpointsSet.TryAdd(ep.ToString(),0);
        string content = Encoding.UTF8.GetString(data);
        string username;
        if (content.IndexOf(EOF)>-1)
        {
            content = content.Split(EOF)[0];
            string[] descriptors = content.Split(SEPARATOR);
            if (descriptors.Length==2 && descriptors[0]==SERVER_PASS)
            {
                username = descriptors[1];
            }
            else
            {
                Logger.Log("erroneous udp login attempt: "
                           + ep + " - " + Encoding.UTF8.GetString(data));
                return;
            }
        }
        else
        {
            Logger.Log("erroneous udp login attempt: " 
                       + ep + " - " + Encoding.UTF8.GetString(data));
            return;
        }
        Logger.Log("udp login - " + numUdpLoginCalls + " - received username: "
                   + username + "\nfrom EP: " + ep + " available: " + _udpClient.Available);
        udpEpToUsernameMapping.TryAdd(ep.ToString(), username);
        if (wholeClientsMapping.ContainsKey(username))
        {
            //todo - theoretically a client could disconnect between these 2 lines but its very rare
            wholeClientsMapping[username].udpEndpoint = ep;
        }
        else
        {
            wholeClientsMapping[username] = new WholeClient(username, ep, null);
        }

        unconfirmedUdps.TryAdd(username,0);
    }

    public static void SendUdpConfirmations()
    {
        if (unconfirmedUdps.Count > 0)
        {
            try
            {
                var copy = new List<string>(unconfirmedUdps.Keys);


                foreach (string u in copy)
                {
                    // check if client is tcp-logged in

                    if (wholeClientsMapping[u].relatedTcpClient != null)
                    {
                        unconfirmedUdps.TryRemove(u,out byte b);
                        SendTcp(wholeClientsMapping[u].relatedTcpClient,
                            (byte) MessageType.UDP_USERNAME_CONFIRM);
                        Logger.Log(
                            string.Format("binded UDP ip {0} to username {1}", wholeClientsMapping[u].udpEndpoint, u));
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Log(e.ToString());
                return;
            }
        }
    }

    public static void DoMatchmaking()
    {
        try
        {
            var keys = new List<string>(clientMapping.Keys);


            foreach (var key in keys)
            {
                if (clientMapping.ContainsKey(key) && clientMapping[key] == null)
                {
                    foreach (var key2 in keys)
                    {
                        //todo an interrupt may come between the &&'s
                        if (!key.Equals(key2) && clientMapping.ContainsKey(key2)
                                              && clientMapping[key2] == null)
                        {
                            if (wholeClientsMapping[key].udpLoginConfirmed &&
                                wholeClientsMapping[key2].udpLoginConfirmed)
                            {
                                clientMapping[key] = key2;
                                clientMapping[key2] = key;
                                relayedClients.TryAdd(key,0);
                                relayedClients.TryAdd(key2,0);
                                Logger.Log(string.Format("users {0} and {1} are now relayed", key, key2));
                                Logger.Log("number of relayed clients: " + relayedClients.Count);
                                SendTcp(wholeClientsMapping[key].relatedTcpClient, (byte) MessageType.YOUR_TURN);
                                SendTcp(wholeClientsMapping[key2].relatedTcpClient, (byte) MessageType.GAME_START);
                                break;
                            }
                        }
                    }
                }

                if (relayedClients.Count > MAX_RELAYED_USERS)
                {
                    break;
                }
            }
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    private static void HandleKeepAliveTimers()
    {
        List<string> keys;
        try
        {
            keys = new List<string>(keepaliveTimerMapping.Keys);


            foreach (string key in keys)
            {
                if ((DateTime.Now - keepaliveTimerMapping[key]).TotalMilliseconds > MAX_TIME_BETWEEN_KEEPALIVES)
                {
                    DisconnectClient(key);
                }
            }
        }
        catch (Exception e)
        {
            Logger.Log(e.ToString());
        }
    }

    private static void OnProcessExit(object sender, EventArgs e)
    {
        Logger.Log("OnProcessExit");
        foreach (var relayedClient in relayedClients.Keys)
        {
            SendTcp(wholeClientsMapping[relayedClient].relatedTcpClient, (byte) MessageType.GAME_ENDED);
        }
    }

    public static int Main(String[] args)
    {
        Logger.Init();
        InitTcpListener();
        _tcpListenThread.Start();
        InitUdp();
        for (int i = 0; i < 10; i++)
        {
            int j = 0;
        }
        _udpClient.BeginReceive(new AsyncCallback(UdpReceiveCallback), null);
        Logger.Log("started UDP client");
        AppDomain.CurrentDomain.ProcessExit += new EventHandler(OnProcessExit);
        AppDomain.CurrentDomain.UnhandledException += OnProcessExit;
        while (true)
        {
            //Listen();
            HandleKeepAliveTimers();
            BulkTcpRead();
            SendUdpConfirmations();
            if (relayedClients.Count < MAX_RELAYED_USERS)
            {
                DoMatchmaking();
            }

            Thread.Sleep(100);
        }

        return 0;
    }
}