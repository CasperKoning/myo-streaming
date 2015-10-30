package nl.ordina.bigdata.myo

import java.io.PrintStream
import java.net.ServerSocket

import scala.io.Source

/**
 * Test server which pushes data to a socket.
 */
object TestDataServer {
  def main(args: Array[String]) {
    val server = new ServerSocket(Constants.DATA_SERVER_PORT)
    val s = server.accept()
    val out = new PrintStream(s.getOutputStream)
    while (true) {
      val file = Source.fromFile(Constants.DATA_PATH+"/raw-myo-data/4334.json")
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
