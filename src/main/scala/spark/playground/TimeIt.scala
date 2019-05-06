package spark.playground

import scala.concurrent.duration._

case class ExecutedBlockInfo[T](what: String,
                                startTime: Deadline,
                                stopTime: Deadline,
                                result: T) {
  lazy val duration: FiniteDuration = stopTime - startTime

  override def toString: String = {
    s"'$what' has been executed in ${(stopTime - startTime).toMillis} ms"
  }
}

object TimeIt {
  def time[T](what: String, body: => T): ExecutedBlockInfo[T] = {
    val startTime = Deadline.now
    val result = body
    val stopTime = Deadline.now
    ExecutedBlockInfo(what, startTime, stopTime, result)
  }

  def log[T](what: String, body: => T, logger: String => Unit = println): T = { //scalastyle:ignore
    val startTime = Deadline.now
    val result = body
    val stopTime = Deadline.now
    logger(ExecutedBlockInfo(what, startTime, stopTime, result).toString)
    result
  }
}