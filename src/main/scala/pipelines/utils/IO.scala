package pipelines.utils

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.mqtt.MqttIO
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PCollection, PDone}

// IO utilities and helpers
object IO {
  val logger = Logging.getLogger(getClass)

  // Return an unbounded collection of bytes from a MQTT topic
  def rawSourceFromMqtt(serverUri: String, topic: String, sc: ScioContext): SCollection[Array[Byte]] = {
    logger.info(s"📡 Reading from topic $topic on MQTT server $serverUri...")
    val source = MqttIO
      .read()
      .withConnectionConfiguration(
        MqttIO.ConnectionConfiguration
          .create(
            serverUri,
            topic
          )
          .withClientId("data-challenge-reader")
      )
    sc.customInput(topic, source)
  }

  // Create a transform Sink to write to a MQTT topic
  // Warning: The MqttIO only fully supports QoS 1 (at least once) consumer downstream may need to dedup
  def rawSinkToMqtt(serverUri: String, topic: String): PTransform[PCollection[Array[Byte]], PDone] = {
    logger.info(s"💾 Sinking to topic $topic on MQTT server $serverUri...")
    MqttIO
      .write()
      .withConnectionConfiguration(
        MqttIO.ConnectionConfiguration
          .create(
            serverUri,
            topic
          )
          .withClientId("data-challenge-writer")
      )
  }

}
