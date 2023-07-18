package pipelines.utils

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.joda.time.Duration

import java.util.UUID

/**
 * Define the parameters for the pipeline extracted from the command line
 * arguments
 *
 * @param scioContext
 * the scio context to run the pipeline (see
 * [[https://spotify.github.io/scio/ScioContext.html ScioContext]])
 * @param driverRatedRawInput
 * the raw input for the DriverRated event as bytes
 * @param driverTippedRawInput
 * the raw input for the DriverTipped event as bytes
 * @param journeyFinishedRawInput
 * the raw input for the JourneyFinished event as bytes
 */
case class ChallengePipelineParams(
                                    scioContext: ScioContext,
                                    intervalDuration: Duration,
                                    driverRatedRawInput: SCollection[Array[Byte]],
                                    driverTippedRawInput: SCollection[Array[Byte]],
                                    journeyFinishedRawInput: SCollection[Array[Byte]],
                                    sink: PTransform[PCollection[Array[Byte]], PDone]
                                  )

object ChallengePipelineParams {
  val logger = Logging.getLogger(getClass)

  // Create the context to run the pipeline from the command line arguments
  def fromArgs(jobName: String, args: Array[String]): ChallengePipelineParams = {
    val pipelineOptions = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[pocChallengeOptions])

    pipelineOptions.setJobName(s"${jobName}-${UUID.randomUUID()}")
    // TODO add GCP labels support
    //    pipelineOptions.setLabels(
    //      Map(
    //        "team" -> labelSanitize("Team Rocket"),
    //        "project" -> labelSanitize("POC test"),
    //        "job" -> labelSanitize(jobName),
    //        "environment" -> labelSanitize("production")
    //      ).asJava
    //    )

    val sc = ScioContext(pipelineOptions)

    logger.info(s"🚀 Pipeline with ${pipelineOptions}...")
    ChallengePipelineParams(
      scioContext = sc,
      driverRatedRawInput = IO.rawSourceFromMqtt(
        serverUri = pipelineOptions.getServerUri,
        topic = pipelineOptions.getDriverRatedTopic,
        sc
      ),
      driverTippedRawInput = IO.rawSourceFromMqtt(
        serverUri = pipelineOptions.getServerUri,
        topic = pipelineOptions.getDriverTippedTopic,
        sc
      ),
      journeyFinishedRawInput = IO.rawSourceFromMqtt(
        serverUri = pipelineOptions.getServerUri,
        topic = pipelineOptions.getSinkDriverStatsTopic,
        sc
      ),
      sink = IO.rawSinkToMqtt(
        serverUri = pipelineOptions.getServerUri,
        topic = pipelineOptions.getSinkDriverStatsTopic
      ),
      intervalDuration = Duration.standardSeconds(pipelineOptions.getAggregationInterval)
    )
  }

}
