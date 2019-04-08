import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import util.Utils;
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;


public class Main {

	public static void main(String[] args) throws Exception {


		String db_name = "bdm";
		String connection_string = "mongodb://bdm:bdm@raichu.fib.upc.es/bdm";

		MongoClient mongoClient = MongoClients.create(connection_string);
		MongoDatabase database = mongoClient.getDatabase(db_name);
		MongoCollection<Document> testColl = database.getCollection("test");

		Document testDoc = new Document()
				.append("name", "Carlos")
				.append("age", 27)
				.append("scores", Arrays.asList(18, 16, 20, 18, 19))
				.append("contact", new Document().append("city", "Barcelona").append("phone", "3348360000"));
		testColl.insertOne(testDoc);

		System.out.println("Number of documents in the collection:");
		System.out.println(testColl.countDocuments());

		System.out.println("Printing the first document:");
		System.out.println(testColl.find().first().toJson());

		System.out.println("Printing all the documents:");
		try (MongoCursor<Document> cursor = testColl.find()
				.projection(fields(include("name"), exclude("_id")))
				.sort(Sorts.ascending("name"))
				.iterator()) {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		}

		System.out.println("Printing all the people with age equals to 32:");
		System.out.println(testColl.find(eq("age", 32)).first().toJson());

		System.out.println("Printing all the people above 25 years:");
		testColl.find(gte("age", 35))
				.projection(fields(include("name"), exclude("_id")))
				.iterator()
				.forEachRemaining(d -> System.out.println(d.toJson()));

		System.out.println("Adding 10 years to all people...");
		//testColl.updateMany(new Document(), inc("age", 10));

		System.out.println("Creating an index on the test collection...");
		//testColl.createIndex(new Document("name", 1));

		MongoCollection<Document> restaurants = database.getCollection("restaurants");
		System.out.println("Printing the number of italian restaurants:");
		restaurants.aggregate(Arrays.asList(
				Aggregates.match(eq("cuisine", "Italian")),
				Aggregates.group("$borough", Accumulators.sum("count", 1))
		)).iterator().forEachRemaining(d -> System.out.println(d.toJson()));

		if (args.length < 1) {
			throw new Exception("Wrong number of parameters, usage: load N (number of documents to create) or Q1/Q2");
		}

		/*
		if (args[0].equals("populate")) {
			if (Utils.isNumber(args[1])) populateDB.populate(Integer.parseInt(args[1]));
		}
		else if (args[0].equals("Q1")) {
			Q1.execute();
		}
		else if (args[0].equals("Q2")) {
			Q2.execute();
		}
		*/

	}
	
}
