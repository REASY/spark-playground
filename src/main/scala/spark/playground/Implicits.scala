package spark.playground

import scala.concurrent.duration._

object Implicits {

  implicit class DurationEx(val self: FiniteDuration) {
    def asTimeSpan: String = {
      val hours = self.toHours
      val minutes = self.toMinutes
      val seconds = self.toSeconds
      val milliseconds = self.toMillis

      "%02d:%02d:%02d.%03d".format(
        hours,
        minutes - new FiniteDuration(hours, HOURS).toMinutes,
        seconds - new FiniteDuration(minutes, MINUTES).toSeconds,
        milliseconds - new FiniteDuration(seconds, SECONDS).toMillis)
    }
  }

  implicit class DoubleEx(val self: Double) {
    def toBoolean: Boolean = self > 0
  }

  implicit class BooleanEx(val self: Boolean) {
    def toDouble: Double = if (self) 1.0 else 0.0
  }
}