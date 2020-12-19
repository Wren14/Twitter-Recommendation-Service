package edu.cmu.cc.group;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.StringUtils;

import edu.cmu.cc.group.q1.DriverQ1;
import edu.cmu.cc.group.q2.DriverQ2;
import edu.cmu.cc.group.q3.DriverQ3;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.sql.SQLException;


public class WebFrontend extends AbstractVerticle {

	ClientSQL client;

	@Override
	public void start(Future<Void> fut) {
		// Create a router object.
		  /*
		  AtomicInteger numCores = new AtomicInteger();
		  AtomicReference<String> mysqlHost=null;
		  AtomicReference<String> mysqlPwd=null;
		  AtomicReference<String> mysqlUser=null;
		  ConfigStoreOptions json = new ConfigStoreOptions().setType("env");
		  ConfigRetrieverOptions options = new ConfigRetrieverOptions().addStore(json);
		  ConfigRetriever retriever = ConfigRetriever.create(vertx, options);
		  retriever.getConfig( ar -> {
					  if (ar.failed()) {
						  // Failed to retrieve the configuration
					  } else {
						  JsonObject config = ar.result();
						  numCores.set(config.getInteger("NUM_CORES"));
						  mysqlHost.set(config.getString("MYSQL_HOST"));
						  mysqlPwd.set(config.getString("MYSQL_PWD"));
						  mysqlUser.set(config.getString("MYSQL_USER"));
					  }
				  });
			*/

		  /*
		JsonObject config = this.config();
		//int numCores = config.getInteger("NUM_CORES");
		private static final String DB_NAME = "final";
		private static final String MYSQL_HOST = System.getenv("MYSQL_HOST");
		//System.out.println("MYSQL_HOST : " + MYSQL_HOST);
		private static final String MYSQL_PWD = System.getenv("MYSQL_PWD");
		//System.out.println("MYSQL_PWD : " + MYSQL_PWD);
		private static final String MYSQL_USER = System.getenv("MYSQL_USER");
		//System.out.println("MYSQL_USER : " + MYSQL_USER);
		private static final String URL = "jdbc:mysql://"+MYSQL_HOST+":3306/" + DB_NAME
				+ "?useSSL=false";
		final JDBCClient client = JDBCClient.createShared(vertx, new JsonObject()
				.put("url", URL)
				.put("driver_class", "com.mysql.jdbc.Driver")
				.put("max_pool_size", 30)
				.put("user", MYSQL_USER)
				.put("password", MYSQL_PWD));

		client.getConnection( conn -> {
			if (conn.failed()) {
				System.err.println(conn.cause().getMessage());
				return;
			}

			connection = conn.result();
		});*/

		client = new ClientSQL();
		try {
			client.initializeConnection();
		} catch (ClassNotFoundException e) {
			System.out.println("Exception in initializing connection " + e);
		}
		catch ( SQLException e) {
			System.out.println("Exception in initializing connection " + e);
		}

		Router router = Router.router(vertx);
		router.get("/").handler(this::healthCheck);
		router.get("/q1").handler(this::getDataQ1);
		router.get("/q2").handler(this::getDataQ2);
		router.get("/q3").handler(this::getDataQ3);
		
		vertx
				.createHttpServer()
				.requestHandler(router::accept)
				.listen(
						// Retrieve the port from the configuration,
						// default to 8080.
						config().getInteger("HTTP_PORT", 80),
						result -> {
							if (result.succeeded()) {
								fut.complete();
							} else {
								fut.fail(result.cause());
							}
						}
				);
	}
	
	private void healthCheck(RoutingContext routingContext) {
		String response = "healthy";
		routingContext.response()
			.putHeader("content-type", "application/text; charset=utf-8")
			.end(response);
	}

	private void getDataQ1(RoutingContext routingContext) {
		String request = routingContext.request().getParam("cc");
		DriverQ1 processor = new DriverQ1();
		try {
			String response = processor.handleInputRequest(request);
			routingContext.response()
					.putHeader("content-type", "application/text; charset=utf-8")
					.end(response);
		} catch (Exception e) {
			//Catch all exceptions and print them to STDERR
			e.printStackTrace();
		}
	}

	private void getDataQ2(RoutingContext routingContext) {

		//TODO - check if getParam() throws an exception if parameter doesn't exist
		String userId = routingContext.request().getParam("user_id");
		String type = routingContext.request().getParam("type");
		String phrase = routingContext.request().getParam("phrase");
		String hashtag = routingContext.request().getParam("hashtag");

		if (StringUtils.isEmpty(userId) ||
				StringUtils.isEmpty(type) ||
				StringUtils.isEmpty(phrase) ||
				StringUtils.isEmpty(hashtag) ) {
			routingContext.response().setStatusCode(400).end();
			return;
		}

		DriverQ2 processor = new DriverQ2();
		try {
			String response = processor.handleInputRequest(client, userId, type, phrase, hashtag);
			routingContext.response()
					.putHeader("content-type", "application/text; charset=utf-8")
					.end(response);
		} catch (Exception e) {
			//Catch all exceptions and print them to STDERR
			e.printStackTrace();
		}
	}
	
	private void getDataQ3(RoutingContext routingContext) {
		try {
			String timeStartStr = routingContext.request().getParam("time_start");
			String timeEndStr = routingContext.request().getParam("time_end");
			String uidStartStr = routingContext.request().getParam("uid_start");
			String uidEndStr = routingContext.request().getParam("uid_end");
			String n1Str = routingContext.request().getParam("n1");
			String n2Str = routingContext.request().getParam("n2");
			
			DriverQ3 processor = new DriverQ3();
			String response = processor.handleQ3(client, timeStartStr, timeEndStr, uidStartStr, uidEndStr, n1Str, n2Str);
			routingContext.response()
				.putHeader("content-type", "application/text; charset=utf-8")
				.end(response);
			
		} catch (Exception e) {
			//Catch all exceptions and print them to STDERR
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		System.out.println("in main function");

		//int numCores = Integer.parseInt(System.getenv("NUM_CORES"));
		int numCores = 1;
		DeploymentOptions options = new DeploymentOptions().setInstances(numCores);
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle("edu.cmu.cc.group.WebFrontend", options);
	}

}