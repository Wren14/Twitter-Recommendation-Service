package edu.cmu.cc.group.query1;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class WebFrontendQ1 extends AbstractVerticle {
	
	  @Override
	  public void start(Future<Void> fut) {
		  // Create a router object.
		  Router router = Router.router(vertx);

		 router.get("/q1").handler(this::getData);
		  
		  vertx
			.createHttpServer()
			.requestHandler(router::accept)
			.listen(
					// Retrieve the port from the configuration,
					// default to 8080.
					config().getInteger("http.port", 80),
					result -> {
						if (result.succeeded()) {
							fut.complete();
						} else {
							fut.fail(result.cause());
						}
					}
			);
	  }
	  
	  private void getData(RoutingContext routingContext) {
		  String request = routingContext.request().getParam("cc");
		  ClientQ1 processor = new ClientQ1();
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

}
