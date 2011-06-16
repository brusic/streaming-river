package streamingriver.plugin

import streamingriver.river.StreamingRiverModule
import org.elasticsearch.plugins.AbstractPlugin
import org.elasticsearch.common.inject.{Inject, Module}
import org.elasticsearch.river.RiversModule

class StreamingRiverPlugin @Inject() extends AbstractPlugin {

  override def name = "streaming-river"

  override def description = "Simple Streaming River Plugin"

  override def processModule(module: Module) = {
    if (module.isInstanceOf[RiversModule]) {
      module.asInstanceOf[RiversModule].registerRiver("streaming", classOf[StreamingRiverModule])
    }
  }
}