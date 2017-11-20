import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;

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

    public long countCollection(String collectionName) {
        return database.getCollection(collectionName).count();
    }

    public void insertWordsUpdate(List<Word> words) {

        MongoCollection<Document> collection = this.database.getCollection("words");

        for (Word word : words) {
            FindIterable<Document> document = collection.find(eq("word", word.getWord()));
            Document newDocument;

            if (document.first() != null) {
                List<Document> times = (List<Document>) document.first().get("times");

                for (Document timeDocument : times) {
                    Date dbTime = timeDocument.getDate("time");
                    int dbFrequency = timeDocument.getInteger("freq");

                    if (dbTime.compareTo(new Date(word.getTimestamp())) != 0) {
                        //Timestamp does not exist in the DB.
                        newDocument = new Document("word", word.getWord());
                        newDocument.append("times", times);
                        times.add(new Document("time", new Date(word.getTimestamp()))
                                .append("freq", word.getFrequency()));
                    } else {
                        //Timestamp does exist in the DB.
                        timeDocument.put("freq", dbFrequency + word.getFrequency());
                        newDocument = new Document("word", word.getWord());
                        newDocument.append("times", times);
                    }
                    collection.replaceOne(new Document("word", word.getWord()), newDocument, new UpdateOptions()
                            .upsert(true));
                    break;
                }
            } else {
                //Create Document because it does not exist.
                newDocument = new Document("word", word.getWord());
                List<Document> dbTimeList = new ArrayList<Document>();
                newDocument.append("times", dbTimeList);
                dbTimeList.add(new Document("time", new Date(word.getTimestamp())).append("freq", word.getFrequency()));
                collection.insertOne(newDocument);
            }
        }
    }

    public void insertWordsMany(List<Word> words) {

        HashMap<String, List<Document>> documentsMap = new HashMap<String, List<Document>>();
        ArrayList<Document> documents = new ArrayList<Document>();

        for (Word word : words) {
            if (!documentsMap.containsKey(word.getWord())) {
                documentsMap.put(word.getWord(), new ArrayList<Document>());
            }

            if (!removeDups(documentsMap.get(word.getWord()), word.getTimestamp(), word.getFrequency())) {
                Document subTimeDoc = new Document()
                        .append("time", word.getTimestamp())
                        .append("freq", word.getFrequency());
                documentsMap.get(word.getWord()).add(subTimeDoc);
            }
        }

        for (String key : documentsMap.keySet()) {
            Document document = new Document()
                    .append("word", key)
                    .append("times", documentsMap.get(key));
            documents.add(document);
        }

        database.getCollection("words").insertMany(documents);

    }

    private boolean removeDups(List<Document> docsWithDups, long timestamp, int freq) {

        for (Document d : docsWithDups) {
            Date date = new Date(d.getLong("time"));
            if ((date.compareTo(new Date(timestamp))) == 0) {
                int newFreq = freq + d.getInteger("freq");
                d.put("freq", newFreq);
                return true;
            }
        }
        return false;
    }

    public Map<Date, Integer> query(String word, Date from, Date to) {
        HashMap<Date, Integer> res = new HashMap<Date, Integer>();
        AggregateIterable<Document> it = this.database.getCollection("words").aggregate(this.aggregates(word, from,
                to));

        for (Document document : it) {
            for (Document d : (List<Document>) document.get("times")) {
                res.put(new Date(d.getLong("time")), d.getInteger("freq"));
            }
        }
        return res;
    }

    private List<Document> aggregates(String word, Date from, Date to) {
        List<Document> filters = new ArrayList<Document>();

        filters.add(new Document("$match", new Document("word", word)));
        filters.add(new Document("$unwind", "$times"));
        filters.add(new Document("$match",
                        new Document("times.time",
                                new Document("$gte", from.getTime())
                                        .append("$lte", to.getTime())
                        )
                )
        );
        filters.add(new Document("$group",
                        new Document("_id", null)
                                .append("times",
                                        new Document("$push",
                                                new Document("freq", "$times.freq")
                                                        .append("time", "$times.time")
                                        )
                                )
                )
        );
        filters.add(new Document("$project",
                        new Document("times", true)
                                .append("_id", false)
                )
        );
        return filters;
    }
}
