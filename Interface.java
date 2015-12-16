package thrift.demo.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;

import thrift.demo.gen.User;
import thrift.demo.gen.UserNotFound;
import thrift.demo.gen.UserService.Iface;

public class UserServiceHandler implements Iface {

	@Override
	public List<User> getUsers() throws TException {
		List<User> list = new ArrayList<User>();
		User user = new User();
		user.setId(1);
		user.setUsername("user1");
		user.setPassword("pwd1");
		list.add(user);
		User user2 = new User();
		user2.setId(1);
		user2.setUsername("user2");
		user2.setPassword("pwd2");
		list.add(user2);
		return list;
	}

	@Override
	public User getUserByName(String username) throws UserNotFound, TException {
		if ("user1".equals(username)) {
			User user = new User();
			user.setId(1);
			user.setUsername("user1");
			user.setPassword("pwd1");
			return user;
		} else {
			throw new UserNotFound();
		}
	}

}
