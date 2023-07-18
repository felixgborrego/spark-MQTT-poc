package pipelines
import pipelines.utils.{ChallengePipelineParams, Logging}

// Main entry point for the Challenge pipeline
// run locally with: sbt "run --serverUri=tcp://localhost:1883 --aggregationInterval=10 --streaming=true"
object Main {
  val logger = Logging.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val pipelineContext = ChallengePipelineParams.fromArgs("data-challenge", args)

    val pipeline = ChallengePipeline
    val stats = pipeline
      .run(
        pipelineContext.scioContext,
        pipelineContext.intervalDuration,
        driverRatedRawInput = pipelineContext.driverRatedRawInput,
        driverTippedRawInput = pipelineContext.driverTippedRawInput,
        journeyFinishedRawInput = pipelineContext.journeyFinishedRawInput
      )

    stats.saveAsCustomOutput("driver-stats-out", pipelineContext.sink)
    logger.info(s"🚀 Starting pipeline ...")
    pipelineContext.scioContext.run().waitUntilFinish()
  }
}
