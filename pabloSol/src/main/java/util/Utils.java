package util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;

public class Utils {

	public static MongoClient getMongo(){

		MongoClientURI uri = new MongoClientURI(
				"mongodb://user:pass@localhost/test");

		MongoClient client = new MongoClient(uri);
		return client;
	}

	public static boolean isNumber(String X) {
		try {
			Integer.parseInt(X);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
}
