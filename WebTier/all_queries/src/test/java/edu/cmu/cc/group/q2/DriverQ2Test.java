package edu.cmu.cc.group.q2;

import edu.cmu.cc.group.q2.DriverQ2;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class DriverQ2Test extends TestCase {
    private final static String TEAM_ID = "Let'sDoIt!";
    private final static String TEAM_AWS_ACCOUNT_ID = "586374175774";


    public DriverQ2Test(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(DriverQ2Test.class);
    }


    public void testValidInputHandling1() {

            DriverQ2 client = new DriverQ2();
            String userId = "580009554";
            String type="retweet";
            String phrase="family";
            String hashtag="northeasthour";


            //String myOutput = client.handleInputRequest(userId, type, phrase, hashtag);


            //assertNotNull(myOutput);

    }
}
