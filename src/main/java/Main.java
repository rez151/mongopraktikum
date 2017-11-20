import java.util.Date;
import java.util.List;
import java.util.Map;

public class Main {


    public static void main(String[] args) {

        MongoManager mongo = new MongoManager("localhost", 27017, "infosysteme");
        mongo.dropCollection("words");
        mongo.createCollection("words");

        long timeBegin = System.nanoTime();
        List<Word> words = Util.readWordsFromFile("/home/reserchr/IdeaProjects/redispraktikum/src/main/java/words.txt");
        mongo.insertWordsMany(words);
        long timeEnd = System.nanoTime();
        long nanos = timeEnd - timeBegin;
        System.out.println("insert Data: " + nanos + " nanos");

        String word = "merkel";
        Date from = new Date(0);
        Date to = new Date();

        timeBegin = System.nanoTime();
        Map<Date, Integer> results = mongo.query(word, from, to);
        timeEnd = System.nanoTime();
        nanos = timeEnd - timeBegin;
        System.out.println("query: " + nanos + " nanos");

        int sum = 0;
        for (int value :results.values()){
            sum += value;
        }

        System.out.println("word " + word + " appeared " + sum + " times between " + from.toString() + " " +
                "and " + to.toString());
    }
}
