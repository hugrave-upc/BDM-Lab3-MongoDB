import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import util.Utils;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class Q1 {

	/**
	 * This is the Java implementation of the following shell-based query
	 * 	db.person.aggregate({$lookup:{from:"company",localField:"worksIn",foreignField:"_id",as:"company"}})
	 *
	 * Check the full reference of available stages for the aggregation framework
	 * in https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
 	 */

	public static void execute() {
		//For each person, its name an its company name

		MongoClient client = Utils.getMongo();
		MongoDatabase database = client.getDatabase("test");
		MongoCollection<Document> personCollection = database.getCollection("person2");
		MongoCollection<Document> companyCollection = database.getCollection("company2");

		personCollection.find().forEach((Block<? super Document>) document -> {
			String companyName = companyCollection.find(eq("_id",document.get("worksIn"))).first().getString("domain");
			System.out.println(document.get("lastName") + " " + companyName);
		});
		client.close();

	}

}
