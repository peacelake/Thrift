package thrift.demo.server;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import thrift.demo.gen.UserService;

public class UserServer {

	public final static int PORT = 8989;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			TNonblockingServerSocket socket = new TNonblockingServerSocket(PORT);
			final UserService.Processor processor = new UserService.Processor(
			        new UserServiceHandler());
			THsHaServer.Args arg = new THsHaServer.Args(socket);
			arg.protocolFactory(new TCompactProtocol.Factory());
			arg.transportFactory(new TFramedTransport.Factory());
			arg.processorFactory(new TProcessorFactory(processor));
			TServer server = new THsHaServer(arg);
			server.serve();
		} catch (TTransportException e) {
			e.printStackTrace();
		}


	}

}
