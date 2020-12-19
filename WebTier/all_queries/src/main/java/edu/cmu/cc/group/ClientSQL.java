package edu.cmu.cc.group;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import edu.cmu.cc.group.q2.DBRow;

public class ClientSQL {
	
	//Query 3 constants
	private final static String
			TWEET_ID = "tweetId",
			IMPACT_SCORE = "impactScore",
			TWEET_TEXT = "tweetText",
			WORD_COUNT = "wordCount";

	private final static int
			TWEET_ID_POS = 1,
			IMPACT_SCORE_POS = 2,
			TWEET_TEXT_POS = 3,
			WORD_COUNT_POS = 4;

	private final static String[] SELECT_FIELDS = new String[] {
			TWEET_ID,
			IMPACT_SCORE,
			TWEET_TEXT,
			WORD_COUNT};
	
	private static final int FETCH_SIZE_Q3 = Integer.parseInt(System.getProperty("FETCH_SIZE_Q3"));
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = System.getProperty("DB_NAME");
    private static final String MYSQL_HOST = System.getProperty("DB_HOSTNAME");
    private static final String MYSQL_PWD = System.getProperty("DB_PASSWORD");
    private static final String MYSQL_USER = System.getProperty("DB_USER");
    private static final String URL = "jdbc:mysql://"+MYSQL_HOST+":3306/" + DB_NAME + "?useSSL=false";
    private static Connection conn;
    private static DataSource myDataSource;

    public void initializeConnection() throws ClassNotFoundException,
            SQLException {
    	
        Class.forName(JDBC_DRIVER);
        
        System.out.println("URL:"+URL);
        System.out.println("DB_USER:"+MYSQL_USER);
        System.out.println("DB_PWD:"+MYSQL_PWD);
        conn = DriverManager.getConnection(URL, MYSQL_USER, MYSQL_PWD);
        
        Properties properties = new Properties();
		properties.put("user", MYSQL_USER);
		properties.put("password", MYSQL_PWD);
        myDataSource = setupDataSource(URL, properties);
        
        
        System.out.println("Connection initialized");
    }

    public void cleanup() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public ArrayList<DBRow> getInfoForUser( String userId) {
        //System.out.println("userId is " + userId);
        ArrayList<DBRow> rows= new ArrayList();
        /*
        String sql = "select user1, hashtagscore, interactionscore, " +
                "replyTexts, replyHashtags, retweetTexts, retweetHashtags, lastestContactTweet, " +
                "user1Name, user1Description from q2 where user2 = '" + userId + "'";

         */
        String sql = "select user1, hashtagscore, interactionscore, " +
                "replyTexts, replyHashtags, retweetTexts, retweetHashtags, lastestContactTweet, " +
                "user1Name, user1Description from q2 where user2=?";
        PreparedStatement stmt = null;
        try {
            //stmt = conn.createStatement();

            stmt = conn.prepareStatement(sql);
            stmt.setString(1,userId);
            stmt.setFetchSize(1000);
            //System.out.println("starting query execution : " + sql);
            ResultSet rs = stmt.executeQuery();
            //System.out.println("query execution done");
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnsNumber = rsmd.getColumnCount();
                //System.out.println("total columns " + columnsNumber);
                //System.out.println("output col1:" + rs.getString(1));
                //System.out.println("output col2:" + rs.getString(2));
                //System.out.println("output col3:" + rs.getString(3));
                //System.out.println("output col4:" + rs.getString(4));
                //System.out.println("output col5:" + rs.getString(5));
                //System.out.println("output col6:" + rs.getString(6));
                //System.out.println("output col7:" + rs.getString(7));
                //System.out.println("output col8:" + rs.getString(8));
                //System.out.println("output col9:" + rs.getString(9));
                //System.out.println("output col9:" + rs.getString(10));
                DBRow row = new DBRow(rs.getString(1), rs.getString(2),
                        rs.getString(3), rs.getString(4), rs.getString(5),
                        rs.getString(6), rs.getString(7), rs.getString(8),
                        rs.getString(9), rs.getString(10));
                rows.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return rows;
    }
    
    
    
	//Retrieve data from DB
	public List<edu.cmu.cc.group.q3.Record> getData(long timeStart, long timeEnd, long uidStart, long uidEnd) throws SQLException {
		
		List<edu.cmu.cc.group.q3.Record> output = new ArrayList<>();
		
		StringBuilder sql = new StringBuilder("SELECT ");
		
		StringJoiner selectFields = new StringJoiner(", ");
		
		for (String field : SELECT_FIELDS)
			selectFields.add(field);
		
		sql.append(selectFields.toString());
		
		sql.append(" FROM tweet WHERE (ts BETWEEN ? AND ?) AND (author BETWEEN ? AND ?) ");
		
		try (Connection myConn = myDataSource.getConnection() ) {
			try (PreparedStatement statement = myConn.prepareStatement(sql.toString())) {
				
				statement.setLong(1, timeStart);
				statement.setLong(2, timeEnd);
				statement.setLong(3, uidStart);
				statement.setLong(4, uidEnd);
	
				statement.setFetchSize(FETCH_SIZE_Q3);
				
				try (ResultSet rs = statement.executeQuery()) {
					while (rs.next()) {
	
						long tweetId = rs.getLong(TWEET_ID_POS);
						int impactScore = rs.getInt(IMPACT_SCORE_POS);
						String tweetText = rs.getString(TWEET_TEXT_POS);
						int wordCount = rs.getInt(WORD_COUNT_POS);
						
						edu.cmu.cc.group.q3.Record record = new edu.cmu.cc.group.q3.Record(tweetId, impactScore, tweetText, wordCount);
						
						output.add(record);
					}
				}
			}
		}
		
		return output;
	}

    public static DataSource setupDataSource(String connectURI, Properties properties) {
		ConnectionFactory connectionFactory =
			new DriverManagerConnectionFactory(connectURI, properties);

		PoolableConnectionFactory poolableConnectionFactory =
			new PoolableConnectionFactory(connectionFactory, null);
		
		poolableConnectionFactory.setMaxOpenPreparedStatements(50);
		
		GenericObjectPoolConfig<PoolableConnection> config =  new GenericObjectPoolConfig<PoolableConnection>();
		config.setMaxTotal(1_000);
		
		
		ObjectPool<PoolableConnection> connectionPool =
				new GenericObjectPool<>(poolableConnectionFactory, config);
		poolableConnectionFactory.setPool(connectionPool);

		PoolingDataSource<PoolableConnection> dataSource =
				new PoolingDataSource<>(connectionPool);

		return dataSource;
	}
    
}
