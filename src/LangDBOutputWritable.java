import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;


public class LangDBOutputWritable implements DBWritable {

    public static final String TABLE_NAME = "output";
    public static final String FIELD_STARTING_PHRASE = "starting_phrase";
    public static final String FIELD_FOLLOWING_WORD = "following_word";
    public static final String FIELD_COUNT = "count";
    private static final int STARTING_PHRASE_INDEX = 1;
    private static final int FOLLOWING_WORD_INDEX = 2;
    private static final int COUNT_INDEX = 3;

    private String startingPhrase;
    private String followingWord;
    private int count;

    public LangDBOutputWritable(String sp, String fw, int c) {
        startingPhrase = sp;
        followingWord = fw;
        count = c;
    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        startingPhrase = rs.getString(STARTING_PHRASE_INDEX);
        followingWord = rs.getString(FOLLOWING_WORD_INDEX);
        count = rs.getInt(COUNT_INDEX);
    }

    @Override
    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(STARTING_PHRASE_INDEX, startingPhrase);
        ps.setString(FOLLOWING_WORD_INDEX, followingWord);
        ps.setInt(COUNT_INDEX, count);
    }
}
