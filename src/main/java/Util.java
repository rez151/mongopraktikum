import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by reserchr on 27.10.17.
 */
public class Util {
    static long toDay(String timestamp) {

        StringBuilder timestampBuilder = new StringBuilder(timestamp);
        while (timestampBuilder.length() < 13) timestampBuilder.append("0");
        timestamp = timestampBuilder.toString();
        //timestamp = timestamp.substring(0, 13);
        long ts = Long.parseLong(timestamp);
        return ts - ts % 86400000;
    }

    public static List<Word> readWordsFromFile(String filePath) {
        List<Word> words = new ArrayList<Word>();
        Reader in;
        try {
            in = new FileReader(filePath);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(in);
            for (CSVRecord record : records) {
                words.add(new Word(record.get(0)));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return words;
    }
}
