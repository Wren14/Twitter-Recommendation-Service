import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;


public class TweetTableMapperTest {
    private Mapper<Object, Text, Text, Text> mapper;
    private MapDriver<Object, Text, Text, Text> driver;

    @Before
    public void setUp() {
        mapper = new TweetTableMapper();
        driver = new MapDriver<>(mapper);
    }

    /**
     * {@code MapDriver.runTest(false)}: the order does not matter.
     *
     * @throws IOException if io exception occurs
     */
    @Test
    public void testTwitterMapper() throws IOException {
    	//1st test: expected no output as this is not a contact tweet
    	//2nd test: expected output with rewteeted user id
    	driver.withInput(new Text(""), new Text("447027565572915200	31 33 39 35 34 31 34 36 35 31 30 30 30 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d c2 a1 41 20 70 6f 72 20 65 6c 20 c3 ba 6c 74 69 6d 6f 21 20 45 73 70 65 72 65 6d 6f 73 20 71 75 65 20 4f 6c 69 76 61 20 66 75 65 73 65 20 62 75 65 6e 61 2e 20 23 47 6f 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 31 33 32 39 36 31 35 32 31 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 6e 6f 6d 61 73 74 65 72 73 5f 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 45 6c 6c 6f 73 20 74 65 6e c3 ad 61 6e 20 61 72 6d 61 73 2c 20 70 65 72 6f 20 6e 6f 73 6f 74 72 6f 73 20 74 65 6e c3 ad 61 6d 6f 73 20 6c c3 a1 70 69 63 65 73 20 79 20 70 61 70 65 6c 2e 20 7c 7c 20 45 73 63 72 69 74 6f 72 61 2c 20 61 20 72 61 74 6f 73 2e 20 7c 7c 20 54 72 69 62 75 74 6f 2e 20 7c 7c 20 45 76 61 6e 65 73 63 65 6e 63 65 2c 20 41 37 58 2c 20 41 45 2c 20 42 53 2c 20 41 41 2c 20 57 54 2c 20 44 62 41 2e 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 6e 75 6c 6c 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 6e 75 6c 6c 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 6e 75 6c 6c 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 6e 75 6c 6c 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 47 6f"))
    		  .withInput(new Text(""), new Text("447028681249017858	31 33 39 35 34 31 34 39 31 37 30 30 30 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 52 54 20 40 67 79 75 74 61 6e 6f 36 6f 36 3a 20 ef bd 9e e7 a8 ae e6 97 8f e3 82 92 e8 b6 85 e3 81 88 e3 81 9f e6 84 9b e3 81 8c e3 80 81 e4 bb 8a e3 81 93 e3 81 93 e3 81 ab ef bd 9e e3 80 80 20 23 e4 b8 80 e7 95 aa e7 9b ae e3 81 ab e3 83 aa e3 83 97 e3 81 8d e3 81 9f e3 82 ad e3 83 a3 e3 83 a9 e3 81 ab e4 ba 8c e7 95 aa e7 9b ae e3 81 ab e3 83 aa e3 83 97 e3 81 8d e3 81 9f e3 82 ad e3 83 a3 e3 83 a9 e3 81 b8 e3 82 ad e3 82 b9 e3 81 95 e3 81 9b e3 82 8b 20 68 74 74 70 3a 2f 2f 74 2e 63 6f 2f 41 67 6d 34 4c 55 6d 7a 53 37 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 32 36 30 35 35 39 34 33 33 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 61 6e 61 6e 61 73 6e 61 73 30 38 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d ef be 8f ef be 9d ef bd b6 ef be 9e e3 80 81 ef bd b9 ef be 9e ef bd b0 ef be 91 e5 a5 bd e3 81 8d e3 81 ae e3 81 8a e3 81 9f e3 81 8f e3 81 a7 e3 81 99 ef bc 81 ef bd b9 ef be 9e ef bd b0 ef be 91 e3 81 af e3 81 a8 e3 81 8f e3 81 ab ef be 83 ef bd b2 ef be 99 ef bd bd ef be 9e e3 81 8c e5 a5 bd e3 81 8d e3 81 a7 e3 80 81 e7 84 a1 e9 a1 9e e3 81 ae e3 81 8a e3 81 a3 e3 81 95 e3 82 93 e5 a5 bd e3 81 8d 28 e7 ac 91 29 e5 9f ba e6 9c ac e3 81 af e4 bd 95 e3 81 a7 e3 82 82 e5 a5 bd e3 81 8d e3 81 a7 e3 81 99 28 5e 5e 29 e5 be 8c e3 81 af 42 4c e3 80 81 47 4c 28 e2 86 90 ef bd b5 ef be 98 ef bd bc ef be 9e ef be 85 ef be 99 e3 81 a0 e3 81 91 29 e3 81 8c e5 a5 bd e3 81 8d e3 81 aa e3 82 93 e3 81 a7 e3 80 81 e8 89 b2 e3 80 85 e5 91 9f e3 81 8f e3 81 a8 e6 80 9d e3 81 84 e3 81 be e3 81 99 28 5e 2d 5e 29 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 6e 75 6c 6c 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 35 33 33 31 31 36 33 34 30 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d 67 79 75 74 61 6e 6f 36 6f 36 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d e3 82 b5 e3 83 a2 e3 83 b3 e3 83 8a e3 82 a4 e3 83 88 e3 81 a8 e3 83 86 e3 82 a4 e3 83 ab e3 82 ba e3 81 99 e3 81 8d e3 81 a7 e3 81 99 e3 80 82 e3 83 9e e3 82 b8 e3 83 90 e3 82 b1 e3 82 82 e3 81 99 e3 81 8d 7b 54 45 41 4d 4c 45 54 27 53 44 4f 49 54 21 4a 4f 49 4e 45 52 7d e4 b8 80 e7 95 aa e7 9b ae e3 81 ab e3 83 aa e3 83 97 e3 81 8d e3 81 9f e3 82 ad e3 83 a3 e3 83 a9 e3 81 ab e4 ba 8c e7 95 aa e7 9b ae e3 81 ab e3 83 aa e3 83 97 e3 81 8d e3 81 9f e3 82 ad e3 83 a3 e3 83 a9 e3 81 b8 e3 82 ad e3 82 b9 e3 81 95 e3 81 9b e3 82 8b"))
    		  .withOutput(new Text("447028681249017858"), new Text("260559433\tnull\t533116340\tRT @gyutano6o6: ～種族を超えた愛が、今ここに～　 #一番目にリプきたキャラに二番目にリプきたキャラへキスさせる http://t.co/Agm4LUmzS7TEAMLETSDOITEOLJEYRAJWRE"))	
    		  .runTest(false);
    }
}
