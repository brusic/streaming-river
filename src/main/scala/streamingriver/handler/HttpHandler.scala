package streamingriver.handler

class HttpHandler(uri: String) extends StreamHandler {
  import dispatch._

  logger.info("creating http handler")

  // Create mutable executor so that it can be restarted via connect (after a close)
  var http: HttpExecutor = _

  override def connect {
    logger.info("Opening HTTP stream for ".format(uri))
    http = new nio.Http
    http(url(uri) ^-- { msg => observers.foreach { observer => observer.handleMessage(msg)}})
  }

  override def close {
    logger.info("Closing HTTP stream for ".format(uri))
    http.shutdown()
  }
}