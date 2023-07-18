package pipelines

import org.joda.time.Instant
object models {

  sealed trait Event {
    def driverId: String

    // Bases timestamp on the event, used to determine the Window interval the event belongs to
    def timestamp: Instant

  }

  case class DriverRatedEvent(
    driverId: String,
    riderId: String,
    journeyId: String,
    rating: Int,
    ratedAt: Instant
  ) extends Event {

    def timestamp = ratedAt

    def toStats = new EventStats(
      driverId = driverId,
      timestamp = ratedAt,
      totalDropOffs = 0,
      totalDriverCancels = 0,
      totalDistance = 0,
      totalPrice = 0,
      totalTips = 0,
      numberOfRatings = 1,
      totalRating = rating.toFloat
    )
  }

  case class DriverTippedEvent(
    driverId: String,
    riderId: String,
    journeyId: String,
    amount: Int,
    tippedAt: Instant
  ) extends Event {
    def timestamp = tippedAt

    def toStats = new EventStats(
      driverId = driverId,
      timestamp = tippedAt,
      totalDropOffs = 0,
      totalDriverCancels = 0,
      totalDistance = 0,
      totalPrice = 0,
      totalTips = amount,
      numberOfRatings = 0,
      totalRating = 0
    )
  }

  case class JourneyFinishedEvent(
    driverId: String,
    riderId: String,
    journeyId: String,
    finishReason: String,
    startedAt: Instant,
    endedAt: Instant,
    distance: Int,
    price: Int
  ) extends Event {
    // assigned to the interval based on the endedAt (no the startedAt)
    def timestamp = endedAt

    def toStats = new EventStats(
      driverId = driverId,
      timestamp = endedAt,
      totalDropOffs = if (finishReason == "DROP_OFF") 1 else 0,
      totalDriverCancels = if (finishReason == "DRIVER_CANCEL") 1 else 0,
      totalDistance = distance,
      totalPrice = price,
      totalTips = 0,
      numberOfRatings = 0,
      totalRating = 0
    )
  }

  case class DriverStatsEvent(
    driverId: String,
    intervalStart: Instant,
    intervalEnd: Instant,
    totalDropOffs: Int,
    totalDriverCancels: Int,
    totalDistance: Int,
    totalPrice: Int,
    totalTips: Int,
    avgRating: Float
  ) extends Event {
    def timestamp = intervalStart
  }

  // Internal Accumulator for the stats of a single driver in a window
  class WindowAccStats(
    var driverId: String,
    var totalDropOffs: Int,
    var totalDriverCancels: Int,
    var totalDistance: Int,
    var totalPrice: Int,
    var totalTips: Int,
    var numberOfRatings: Int,
    var totalRating: Float
  ) {
    def avgRating = if (numberOfRatings > 0) totalRating / numberOfRatings else 0

    def applyEventStats(event: EventStats) = {
      totalRating += event.totalRating
      numberOfRatings += event.numberOfRatings
      totalTips += event.totalTips

      totalDropOffs += event.totalDropOffs
      totalDriverCancels += event.totalDriverCancels

      totalDistance += event.totalDistance
      totalPrice += event.totalPrice

      this
    }
  }

  case class EventStats(
    driverId: String,
    timestamp: Instant,
    totalDropOffs: Int,
    totalDriverCancels: Int,
    totalDistance: Int,
    totalPrice: Int,
    totalTips: Int,
    numberOfRatings: Int,
    totalRating: Float
  ) {
    def avgRating = if (numberOfRatings > 0) totalRating / numberOfRatings else 0
  }

}
