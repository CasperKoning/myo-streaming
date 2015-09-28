package nl.ordina.bigdata.myo

import java.io.PrintStream
import java.net.ServerSocket

import scala.io.Source

/**
 * Test server which pushes data to a socket.
 */
object TestDataServer {
  def main(args: Array[String]) {
    val server = new ServerSocket(Constants.dataServerPort)
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream)
    while (true) {
      val file = Source.fromFile(Constants.dataPath)
      val in = file.getLines()
      in.next(); //skip header
      while (in.hasNext) {
        out.println(in.next())
        out.flush()
      }
    }
    s.close()
  }
}
