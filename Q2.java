import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Filters.*;

public class Q2 {

	public static void execute() {
		// Connect to the database
		String db_name = "bdm";
		String connection_string = "mongodb://bdm:bdm@raichu.fib.upc.es:27017/bdm";

		MongoClient mongoClient = MongoClients.create(connection_string);
		MongoDatabase database = mongoClient.getDatabase(db_name);
		MongoCollection<Document> peopleColl = database.getCollection("People");
		MongoCollection<Document> companiesColl= database.getCollection("Companies");

		/*
		companiesColl
				.find()
				.projection(fields(include("name", "_id")))
				.iterator()
				.forEachRemaining(company -> {
					long count = peopleColl.count(eq("company_id", company.get("_id")));
					System.out.println(company.get("name") + ": " + String.valueOf(count));
		});
		*/

		Map<String, Document> companiesMap = new HashMap<>();
		companiesColl.find().iterator()
				.forEachRemaining(comp -> companiesMap.put(comp.getString("_id").trim(), comp));
		peopleColl.aggregate(Arrays.asList(
				Aggregates.sortByCount("$company_id"),
				Aggregates.limit(1000)
		)).iterator().forEachRemaining(agg -> {
			if (companiesMap.containsKey(agg.getString("_id").trim())) {
				String compName = companiesMap.get(agg.getString("_id")).getString("name");
				System.out.println(compName + ": " + String.valueOf(agg.getInteger("count")));
			}
		});

	}
}
