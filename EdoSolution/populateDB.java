import com.devskiller.jfairy.*;
import com.devskiller.jfairy.producer.company.Company;
import com.devskiller.jfairy.producer.person.Person;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;

import java.util.ArrayList;
import java.util.List;

public class populateDB {

	public static void populate(int N) {
		Fairy fairy = Fairy.create();
		// Connect to the database
		String db_name = "vdm_training";
		String connection_string = "mongodb://hugrave:bdm123@ds159591.mlab.com:59591/vdm_training";

		MongoClient mongoClient = MongoClients.create(connection_string);
		MongoDatabase database = mongoClient.getDatabase(db_name);
		MongoCollection<Document> peopleColl = database.getCollection("People");
		MongoCollection<Document> companiesColl= database.getCollection("Companies");

		List<Document> peopleList = new ArrayList<>();
		List<Document> companiesList = new ArrayList<>();

		for (int i=0; i < N; i++) {
			Person persFairy = fairy.person();
			Company compFairy = persFairy.getCompany();
			Document personDoc = createPerson(persFairy);
			Document companyDoc = companiesColl.find(eq("_id", compFairy.getVatIdentificationNumber())).first();
			if (companyDoc == null) {
				// Company not found, add it
				companyDoc = createCompany(compFairy);
				companiesList.add(companyDoc);
			}
			peopleList.add(personDoc);
		}

		// Inserting companies and people in Mongo
		peopleColl.insertMany(peopleList);
		companiesColl.insertMany(companiesList);
	}

	private static Document createPerson(Person p) {
		return new Document()
				.append("_id", p.getPassportNumber())
				.append("first_name", p.getFirstName())
				.append("last_name", p.getLastName())
				.append("age", p.getAge())
				.append("telephone", p.getTelephoneNumber())
				.append("email", p.getCompanyEmail())
				.append("company_id", p.getCompany().getVatIdentificationNumber());
	}

	private static Document createCompany(Company c) {
		return new Document()
				.append("_id", c.getVatIdentificationNumber())
				.append("email", c.getEmail())
				.append("domain", c.getDomain())
				.append("name", c.getName())
				.append("URL", c.getUrl());
	}

}