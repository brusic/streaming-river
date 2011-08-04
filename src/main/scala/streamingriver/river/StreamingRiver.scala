package streamingriver.river

import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.elasticsearch.common.xcontent.support.XContentMapValues
import org.elasticsearch.indices.IndexAlreadyExistsException
import org.elasticsearch.river.{AbstractRiverComponent, River, RiverName, RiverSettings}
import streamingriver.handler.StreamHandler
import org.elasticsearch.client.{Requests, Client}

class StreamingRiver @Inject()(name: RiverName, settings: RiverSettings, client: Client)
  extends AbstractRiverComponent(name, settings)
  with River {

  logger.info("creating streaming river for %s".format(riverName.name))

  val config = createConfiguration(settings)
  val stream: Option[StreamHandler] = config.buildStream
  val executor: Option[RequestExecutor] = config.buildIndexExectuor

  override def close() {
    logger.info("closing streaming river [%s]".format(riverName.name))

    stream match {
      case Some(s) => s.close
      case None => logger.warn("Stream for %s inexistant".format(riverName.name))
    }
  }

  override def start() {
    stream match {
      case Some(s) =>

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

        s.addObserver(this)
        s.connect
      case None => logger.warn("Stream for %s inexistant".format(riverName.name))
    }

  }

  /**
   * 'uri' is the only required parameter
   */
  private def createConfiguration(settings: RiverSettings): Configuration = {
    // switch to a builder pattern if the code gets too unruly

    var indexName = riverName.name
    var typeName = "streaming"

    var mapping: String = null
    var idField: String = null
    var bulkSize = 0
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

      if (settings.settings.containsKey("bulk")) {
        val xxx = XContentMapValues.nodeIntegerValue(settings.settings.get("bulk"), 0)
      }

      if (settings.settings().containsKey("index")) {
        val indexSettings = settings.settings.get("index").asInstanceOf[java.util.Map[String, Object]]
        indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name)
        typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "streaming-type")

      } else {
        indexName = riverName.name()
        typeName = "streaming-type"
      }

      bulkSize = XContentMapValues.nodeIntegerValue(settings.settings().get("bulk_size"), 0)
      dropThreshold = XContentMapValues.nodeIntegerValue(settings.settings().get("drop_threshold"), 10)


    }
    Configuration(indexName, typeName, uri, mapping, idField, bulkSize, dropThreshold)
  }


  def handleMessage(msg: String) {
    logger.debug("received message [size: %s]: %s ...".format(msg.size, msg.take(16)))

    executor match {
      case Some(requestExecutor) =>
        try {
          val indexRequest = Requests.indexRequest(config.indexName).`type`(config.typeName)

          val parser = JsonXContent.jsonXContent.createParser(msg)
          val msgMap = parser.map

          // set the id if a field has been defined and the value can be found in the JSON
          if (config.idField!= null && msgMap.get(config.idField) != null) {
            indexRequest.id(msgMap.get(config.idField).toString)
          }


          // TODO: make create configurable?
          requestExecutor.act(indexRequest.create(false).source(msg))

        } catch {
          case e: Exception =>
            logger.error("failed to construct index request [%s] from %s".format(riverName.name, msg), e)
        }

      case None => logger.warn("No executor defined for %s".format(riverName.name))
    }
  }

  // Simple configuration encapsulation - don't want to think too much about it
  private[StreamingRiver] case class Configuration(indexName: String, typeName: String, uri: String,
                                                 mapping: String, idField: String, bulkSize: Int, dropThreshold: Int) {
    def buildStream: Option[StreamHandler] = StreamHandler(uri)
    def buildIndexExectuor: Option[RequestExecutor] = {
      if (bulkSize>0) Some(new BulkRequestExecutor(client, bulkSize, dropThreshold)) else Some(new SingleRequestExecutor(client))
    }
  }

}
