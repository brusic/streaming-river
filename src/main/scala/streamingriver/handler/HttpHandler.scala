package streamingriver.handler

class HttpHandler(uri: String) extends StreamHandler {
  import dispatch._

  logger.info("creating http handler")

  // Create mutable executor so that it can be restarted via connect (after a close)
  var h: HttpExecutor = _

  override def connect = {
    logger.info("Opening HTTP stream for ".format(uri))
    h = new nio.Http
    h(url(uri) ^-- { msg => observers.foreach { listener => listener.handleMessage(msg)}})
  }

  override def close = {
    logger.info("Closing HTTP stream for ".format(uri))
    h.shutdown()
  }
}