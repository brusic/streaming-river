package streamingriver.river

import java.util.concurrent.atomic.AtomicInteger
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.action.index.IndexRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.common.logging.ESLoggerFactory
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}

trait RequestExecutor {
  def act(request: IndexRequest)
}

/**
 * Wrapper abstraction around a BulkRequestBuilder instance with an interface to add indexrequest objects
 * Execute a bulk index once we pass a configurable number of requests
 */
class BulkRequestExecutor(client: Client, bulkSize: Int, dropThreshold: Int) extends RequestExecutor {
  private val logger = ESLoggerFactory.getLogger(this.getClass.getSimpleName)

  private var underlyingBuilder = client.prepareBulk
  private val onGoingBulks = new AtomicInteger

  def act(request: IndexRequest) {
    underlyingBuilder.add(request)
    processBulkIfNeeded()
  }

  private def processBulkIfNeeded() {
    if (underlyingBuilder.numberOfActions() >= bulkSize) {
      // execute the bulk operation
      val currentOnGoingBulks = onGoingBulks.incrementAndGet()
      if (currentOnGoingBulks > dropThreshold) {
        onGoingBulks.decrementAndGet()
        logger.warn("dropping bulk, [{}] crossed threshold [{}]", onGoingBulks, new java.lang.Integer(dropThreshold))
      } else {
        try {
          underlyingBuilder.execute(new ActionListener[BulkResponse]() {
            def onResponse(bulkResponse: BulkResponse) {
              import scala.collection.JavaConversions._
              logger.debug("Executed bulk index of size: %s. Actual: %s".format(bulkSize, bulkResponse.items.size))

              if (bulkResponse.hasFailures) {
                logger.warn("Bulk index failure: %s".format(bulkResponse.buildFailureMessage))
              }

              val count = bulkResponse.iterator.count { bulkItemResponse => bulkItemResponse.version > 1 }
              if (count>0) {
                logger.debug("Bulk index changed %s items".format(count))
              }

              onGoingBulks.decrementAndGet()
            }

            def onFailure(e: Throwable) {
              logger.warn("failed to execute bulk", e)
            }
          })
        } catch {

          case e: Exception =>
            logger.warn("failed to process bulk", e)
        }
      }

      // reset the builder
      underlyingBuilder = client.prepareBulk
    }
  }
}

/**
 * Simple pass-thru
 */
class SingleRequestExecutor(client: Client) extends RequestExecutor {
  private val logger = ESLoggerFactory.getLogger(this.getClass.getSimpleName)

  def act(request: IndexRequest) {
    try {
      client.index(request, new ActionListener[IndexResponse]() {
        def onResponse(indexResponse: IndexResponse) {
          logger.debug("index executed. Update? %s".format(indexResponse.version > 1))
        }

        def onFailure(e: Throwable) {
          logger.error("failed to execute index [%s]".format(request), e)
        }
      })
    } catch {
      case e: Exception =>
        logger.warn("failed to execute index [%s]".format(request), e)
    }
  }
}