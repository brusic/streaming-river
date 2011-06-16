package streamingriver.river

import org.elasticsearch.common.inject.AbstractModule
import org.elasticsearch.river.River

class StreamingRiverModule extends AbstractModule {

    override def configure = bind(classOf[River]).to(classOf[StreamingRiver]).asEagerSingleton()
}
