import java.util.Date;
import java.util.List;
import java.util.Map;

public class Main {


    public static void main(String[] args) {

        MongoManager mongo = new MongoManager("localhost", 27017, "infosysteme");
        mongo.dropCollection("words");
        mongo.createCollection("words");

        List<Word> words = Util.readWordsFromFile("/home/reserchr/IdeaProjects/redispraktikum/src/main/java/words.txt");
        mongo.insertWords(words);
        Map<Date, Integer> hallo = mongo.query(
                "hallo",
                new Date(Util.toDay("1006211872000")),
                new Date(Util.toDay("1623072919000"))
        );
    }
}
