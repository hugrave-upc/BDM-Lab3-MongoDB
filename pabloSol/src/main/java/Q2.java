import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.person.Person;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import util.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static com.mongodb.client.model.Filters.eq;

public class Q2 {

	/**
	 * This is the Java implementation of the following shell-based query
	 * 	db.company.aggregate([{$lookup:{from:"person",localField:"_id",foreignField:"worksIn",as:"employees"}},
	 * 		{$project:{_id:1,employees:{$size:"$employees"}}}])
	 *
	 * Check the full reference of available stages for the aggregation framework
	 * in https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
	 */

	public static void execute() {
		// For each company, the name and the number of employees.

		MongoClient client = Utils.getMongo();
		MongoDatabase database = client.getDatabase("test");
		MongoCollection<Document> personCollection = database.getCollection("person2");
		MongoCollection<Document> companyCollection = database.getCollection("company2");

		personCollection.aggregate(Arrays.asList(
				Aggregates.group("$worksIn", Accumulators.sum("count", 1))
		)).forEach((Block<? super Document>) document -> {
			String companyName = companyCollection.find(eq("_id",document.getString("_id"))).first().getString("domain");
			System.out.println(companyName + " " + document.getInteger("count"));

		});
		client.close();

	}
}
