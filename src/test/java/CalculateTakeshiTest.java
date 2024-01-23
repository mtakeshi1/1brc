import dev.morling.onebrc.CalculateAverage_mtakeshi;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URL;

public class CalculateTakeshiTest {

    static String run(String input) throws IOException {
        var bout = new ByteArrayOutputStream();

        var old = System.out;
        try (var pw = new PrintStream(bout)) {
            System.setOut(pw);
            CalculateAverage_mtakeshi.main(new String[]{ input });
        }
        finally {
            System.setOut(old);
        }
        return bout.toString();
    }

    public static String getFileResource(String resource) {
        URL input = CalculateTakeshiTest.class.getResource(resource);
        if (input == null || !input.getProtocol().equals("file")) {
            throw new RuntimeException(resource);
        }
        return input.getPath();
    }

    public void testSample(Object suffix) throws Exception {
        String inputFile = getFileResource(STR."samples/measurements-\{suffix}.txt");
        String outputFile = getFileResource(STR."samples/measurements-\{suffix}.out");
        String response = run(inputFile);
        try (var br = new BufferedReader(new FileReader(outputFile))) {
            String line = br.readLine();
            Assertions.assertEquals(line + "\n", response);
        }
    }

    @Test
    public void test1() throws Exception {
        testSample(1);
    }

    @Test
    public void test2() throws Exception {
        testSample(2);
    }

    @Test
    public void test3() throws Exception {
        testSample(3);
    }

    @Test
    public void test10() throws Exception {
        testSample(10);
    }

    @Test
    public void test20() throws Exception {
        testSample(20);
    }

    @Test
    public void test10000() throws Exception {
        testSample("10000-unique-keys");
    }

    @Test
    public void testBoundaries() throws Exception {
        testSample("boundaries");
    }

    @Test
    public void testComplexUTF8() throws Exception {
        testSample("complex-utf8");
    }

    @Test
    public void testDot() throws Exception {
        testSample("dot");
    }

    @Test
    public void testRounding() throws Exception {
        testSample("rounding");
    }

    @Test
    public void testShort() throws Exception {
        testSample("short");
    }

    @Test
    public void testShortest() throws Exception {
        testSample("shortest");
    }
}
