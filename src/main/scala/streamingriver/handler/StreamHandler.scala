package streamingriver.handler

import org.elasticsearch.common.logging.ESLoggerFactory


abstract class StreamHandler {
  type Observer = {
    def handleMessage(msg: String)
  }

  val logger = ESLoggerFactory.getLogger(this.getClass.getCanonicalName)

  var observers = List[Observer]()
  def addObserver(observer: Observer) { observers = observer +: observers }

  def connect

  def close
}

object StreamHandler {
  val ProtocolRegex = "([a-zA-Z]*?)://.*".r

  def apply(url: java.net.URL): Option[StreamHandler] = apply(url.toString)
  def apply(uri: java.net.URI): Option[StreamHandler] = apply(uri.toString)

  def apply(url: String): Option[StreamHandler] = {
    url match {
      case ProtocolRegex("http" | "https") => Some(new HttpHandler(url))
      case ProtocolRegex("ws" | "wss") => Some(new WebSocketHandler(url))
      case _ => None
    }
  }
}
