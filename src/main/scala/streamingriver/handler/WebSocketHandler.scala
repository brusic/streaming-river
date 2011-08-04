package streamingriver.handler

import client.WebSocketClient
import client.WebSocket._

class WebSocketHandler(uri: String) extends StreamHandler {

  logger.info("creating websocket handler")

  val wsc = new WebSocketClient(new java.net.URI(uri))({
    case OnOpen => logger.info("opening websocket client for [%s]".format(uri))
    case OnMessage(msg) => observers.foreach { listener => listener.handleMessage(msg)}
  })

  // delegate to websocket client
  override def connect = {
    logger.info("Opening WebSocket for %s".format(uri))
    wsc.connect
  }

  override def close = {
    logger.info("Closing WebSocket for %s".format(uri))
    wsc.close
  }
}