package pipelines.utils

import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.slf4j.LoggerFactory
import pipelines.models.EventStats

object Logging {

  def getLogger(name: String) = LoggerFactory.getLogger(name)

  def getLogger(clazz: Class[_]) = LoggerFactory.getLogger(clazz)

  def watermarkLoggingIntervalFn[A] = ParDo.of(new WatermarkLogging[A])

  // Log the watermark and window information for each element
  // to help troubleshoot windowing issues
  class WatermarkLogging[A] extends DoFn[(A, IntervalWindow), (A, IntervalWindow)] {
    val logger = LoggerFactory.getLogger(getClass)
    @ProcessElement
    def processElement(ctx: DoFn[(A, IntervalWindow), (A, IntervalWindow)]#ProcessContext): Unit = {
      val data = ctx.element()
      val watermark = ctx.timestamp()

      logger.debug(
        s"Watermark: $watermark, Window: ${data._2}, pane: " + ctx.pane()
      )
      if (ctx.timestamp().isAfter(data._2.maxTimestamp())) {
        logger.debug(s"Watermark passed maxTimestamp of window, and will be closed.end: ${data._2.end()}")
      }
      ctx.output(data)
    }
  }
}
