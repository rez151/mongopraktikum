/**
 * Created by reserchr on 09.10.17.
 */
class Word {
    private String word;
    private int frequency;
    private long timestamp;

    Word(String line) {
        String[] lineElements = line.split(" {2}");
        this.word = lineElements[0];
        this.frequency = Integer.parseInt(lineElements[1]);
        this.timestamp = Util.toDay(lineElements[2]);
    }

    String getWord() {
        return word;
    }

    int getFrequency() {
        return frequency;
    }

    long getTimestamp() {
        return timestamp;
    }
}
