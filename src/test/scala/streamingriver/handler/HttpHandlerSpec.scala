package streamingriver.handler

import org.specs.Specification
import org.specs.util.TimeConversions._

class HttpHandlerSpec extends Specification {

  // requires a "chatty" stream in order to not wait long for the test
  val TEST_URL = "http://stream.meetup.com/2/rsvps"

  var handler: HttpHandler = _

  "A 'HttpHandler'" should {
    "A 'HttpHandler' should stream" in {
      var count = 0
      handler = new HttpHandler(TEST_URL)
      handler.addObserver(new Object { def handleMessage(msg: String) { count = count + 1} } )
      handler.connect
      count must eventually(2, 10.seconds)(beGreaterThan(0))
      handler.close
    }
  }
}
