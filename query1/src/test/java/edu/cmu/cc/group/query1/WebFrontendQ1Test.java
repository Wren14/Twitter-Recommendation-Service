package edu.cmu.cc.group.query1;


import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class WebFrontendQ1Test {

	private Vertx vertx;
	int port = 80;

	@Before
	public void setUp(TestContext context) {
	  vertx = Vertx.vertx();
	  DeploymentOptions options = new DeploymentOptions()
		  .setConfig(new JsonObject().put("http.port", port)
	  );
	  vertx.deployVerticle(WebFrontendQ1.class.getName(),
		  context.asyncAssertSuccess());
	}

	@After
	public void tearDown(TestContext context) {
	  vertx.close(context.asyncAssertSuccess());
	}

	@Test
	public void testMyApplication(TestContext context) {
	  /*final Async async = context.async();
	  
	  vertx.createHttpClient().getNow(port, "localhost", "/q1?cc=eJyVktFOwzAMRX9lyjMPdmLHCb-CEOralBVt7UQLQ5r677jbVFJQkZan6DqRT058NuWuaFrzuHk6m2K_fxm-rvv3VH7qjiECMkr0Eh82pjgMl_C2NBmaQ9LIIDOIRT0nEp23DshoeVf0u6lcMSS23pvxWdOm0gyyMtgQ4zbE6cqxO03RseuHY9cmMz5slmh9aqu_aDdgCgRMji16nIHR-kAhktekTumSUAir-BGFFvhc2C1VglPWN69TH--CthJxjBfCW3-0gRAhRBVwhzGypNW8pURLJXPKjGFurC4TY4mZsbr7aIfpM9eM_Wb7TxkH4cgRZ2MMtCqMSIAW9Fty4qSof4RpX4oCQF5syI05QusYdGTcHb6EUPxiwgQ4CeW-bO7Lece1SObrLRVtfz3eptNV1kwV3DQH-qwQZcYiB56sfvoqVtDXgRnHbykf0R8=",
	   response -> {
		response.handler(body -> {
			System.out.println(body.toString());
			context.assertTrue(body.toString().contains("Let'sDoIt"));
			async.complete();
		});
	  });*/
	  
	  /*
	  vertx.createHttpClient().getNow(port, "localhost", "/",
			  response -> {
			   response.handler(body -> {
				 context.assertTrue(body.toString().contains("Hello"));
				 async.complete();
			   });
			 });*/
	}
}
