package streamingriver.river

import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.common.xcontent.support.XContentMapValues
import org.elasticsearch.indices.IndexAlreadyExistsException
import org.elasticsearch.river.{AbstractRiverComponent, River, RiverName, RiverSettings}
import streamingriver.handler.StreamHandler

class StreamingRiver @Inject()(name: RiverName, settings: RiverSettings, client: Client)
  extends AbstractRiverComponent(name, settings)
  with River {

  private[StreamingRiver] case class StreamingRiverConfiguration(indexName: String, typeName: String, uri: String,
                                                 mapping: String, idField: String, bulkSize: Int, dropThreshold: Int)

  logger.info("creating streaming river for %s".format(riverName.name))

  var stream: StreamHandler = null
  val config = createConfiguration(settings)

  // null config indicates an invalid uri (the only required param
  if (config!=null) {
    stream = StreamHandler(config.uri) match {
      case Some(handler) => handler
      case None =>
        logger.error("No handler defined for %s".format(config.uri))
        null
    }
  }

  override def close() {
    logger.info("closing streaming river [%s]".format(riverName.name))
    if (stream != null) {
      stream.close
    }
  }

  override def start() {
    if (stream == null) {
      return
    }

    logger.info("starting streaming river [%s]".format(riverName.name))

    try {
      if (config.mapping!=null) {
        client.admin.indices.prepareCreate(config.indexName).addMapping(config.typeName, config.mapping).execute.actionGet
      } else {
        client.admin.indices.prepareCreate(config.indexName).execute.actionGet
      }

    } catch {
      case e: Exception =>
        if (ExceptionsHelper.unwrapCause(e).isInstanceOf[IndexAlreadyExistsException]) {
          // that's fine
        } else if (ExceptionsHelper.unwrapCause(e).isInstanceOf[ClusterBlockException]) {
        } else {
          logger.warn("failed to create index %s, disabling streaming river [%s]: %s".format(config.indexName, riverName.name, e))
          return
        }
    }

    stream.addObservers(this)
    stream.connect
  }

  private def createConfiguration(settings: RiverSettings): StreamingRiverConfiguration = {
    // switch to a builder pattern if the code gets too unruly

    var indexName = riverName.name
    var typeName = "streaming"

    var mapping: String = null
    var idField: String = null
    var bulkSize = 100
    var dropThreshold = 10

    val uri = XContentMapValues.nodeStringValue(settings.settings.get("uri"), null)
    if (uri==null) {
      // the URI is the only required parameter
      logger.error("no uri specified, disabling river [%s]".format(riverName.name))
      return null
    } else {
      idField = XContentMapValues.nodeStringValue(settings.settings.get("id_field"), null)

      if (settings.settings.containsKey("mapping")) {
        val mappingSettings = settings.settings.get("mapping").asInstanceOf[java.util.Map[String, Object]]
        val builder = XContentFactory.jsonBuilder.map(mappingSettings)
        mapping = """{"%s":{"properties":%s}}""".format(typeName, builder.string)
      }

      if (settings.settings().containsKey("index")) {
        val indexSettings = settings.settings.get("index").asInstanceOf[java.util.Map[String, Object]]
        indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name)
        typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "streaming-type")

      } else {
        indexName = riverName.name()
        typeName = "streaming-type"
      }

      bulkSize = XContentMapValues.nodeIntegerValue(settings.settings().get("bulk_size"), 100)
      dropThreshold = XContentMapValues.nodeIntegerValue(settings.settings().get("drop_threshold"), 10)


    }
    StreamingRiverConfiguration(indexName, typeName, uri, mapping, idField, bulkSize, dropThreshold)
  }


  def handleMessage(msg: String) = {
    logger.debug("got message: ".format(msg))

    try {
      val parser = JsonXContent.jsonXContent.createParser(msg)
      val msgMap = parser.map

      var indexBuilder = client.prepareIndex(config.indexName, config.typeName).setSource(msg)

      // set the id if a field has been defined and the value can be found in the JSON
      if (config.idField!= null && msgMap.get(config.idField) != null) {
        indexBuilder = indexBuilder.setId(msgMap.get(config.idField).toString)
      }

      indexBuilder.execute(new ActionListener[IndexResponse]() {
        def onResponse(indexResponse: IndexResponse) {
          logger.debug("index executed: %s %s".format(indexResponse.getId, indexResponse.getIndex))
        }

        def onFailure(e: Throwable) {
          logger.error("failed to execute index [%s] from %s".format(riverName.name, msg), e)
        }
      })

    } catch {
      case e: Exception =>
        logger.error("failed to construct index request [%s] from %s".format(riverName.name, msg), e)
    }
  }
}
