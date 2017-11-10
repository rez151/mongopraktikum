import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.*;

/**
 * Created by reserchr on 02.11.17.
 */
public class MongoManager {

    private MongoClient mongo;
    private MongoDatabase database;

    public MongoManager(String ip, int port, String dbname) {

        // Creating a Mongo client
        mongo = new MongoClient(ip, port);
        System.out.println("Connected to the database successfully");

        // Accessing the database
        database = mongo.getDatabase(dbname);
    }

    public void createCollection(String collectionName) {
        this.database.createCollection(collectionName);
        System.out.println("collection " + collectionName + " created successfully");
    }

    public void dropCollection(String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.drop();
        System.out.println("collection " + collectionName + " dropped successfully");
    }

    public void insertWords(List<Word> words) {

        MongoCollection<Document> collection = database.getCollection("words");
        List<Document> documents = new ArrayList<Document>();

        for (Word word : words) {

            Document document = new Document();
            document.put("word", word.getWord());

            Document time = new Document();
            time.put("time", word.getTimestamp());
            time.put("frequency", word.getFrequency());
            document.put("times", time);

            documents.add(document);
        }
        collection.insertMany(documents);

/*        collection.findOneAndUpdate(Filters.eq("word", "hallo"), new Document("$inc", new Document("times",
                new Document("time", 123456))));*/

    }

    public long countCollection(String collectionName) {
        return database.getCollection(collectionName).count();
    }

    public Map<Date, Integer> query(String word, Date from, Date to) {

        MongoCollection words = database.getCollection("words");

        words.findOneAndUpdate(Filters.eq("word", "hallo"), new Document("$push", new Document("times",
                new Document("time", 123456))));

        FindIterable word1 = words.find(Filters.eq("word", word));

        for (Object o : word1) {
            System.out.println(o.toString());
        }


        FindIterable findIterable = words.find(Filters.all(
                "word", word,
                "year", "2015"));
        /*FindIterable findIterable = words.find(Filters.and(
                Filters.gte("year", toY),
                Filters.lte("year", fromY)));
*/
        MongoCursor iterator = findIterable.iterator();
        Document doc = new Document();

        while (iterator.hasNext()) {
            doc = (Document) iterator.next();
        }

        return null;
    }
}
