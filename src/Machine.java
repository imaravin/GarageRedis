public class Machine
{
    String address;
    int port;

    public Machine(String address, int port)
    {
        this.address = address;
        this.port = port;
    }

    @Override
    public String toString()
    {
        return address+" : "+port;
    }

}
