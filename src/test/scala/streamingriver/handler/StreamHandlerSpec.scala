package streamingriver.handler

import org.specs.Specification

class StreamHandlerSpec extends Specification {

  "'StreamHandler' factory must return a valid Handler" in {
    StreamHandler(new java.net.URI("http://stream.example.com")) must beSome[StreamHandler]
    StreamHandler(new java.net.URI("ws://socket.example.com")) must beSome[StreamHandler]
  }

  "'StreamHandler' factory must return None for a invalid uri" in {
    StreamHandler(new java.net.URI("ftp://ftp.example.com")) must beNone
  }

}