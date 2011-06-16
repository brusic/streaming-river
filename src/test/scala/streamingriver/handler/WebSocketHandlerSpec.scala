package streamingriver.handler

import org.specs.Specification
import org.specs.util.TimeConversions._

class WebSocketHandlerSpec extends Specification {

  // requires a "chatty" stream in order to not wait long for the test
  val TEST_URL = "ws://stream.meetup.com/2/rsvps"

  var handler: WebSocketHandler = _

  "A 'WebSocketHandler'" should {
    "A 'WebSocketHandler' should stream" in {
      var count = 0
      handler = new WebSocketHandler(TEST_URL)
      handler.addObservers(new Object { def handleMessage(msg: String) { count = count + 1} } )
      handler.connect
      count must eventually(2, 10.seconds)(beGreaterThan(0))
      handler.close
    }
  }
}