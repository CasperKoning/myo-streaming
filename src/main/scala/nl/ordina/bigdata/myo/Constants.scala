package nl.ordina.bigdata.myo

object Constants {
  val LABEL = "label"
  val FEATURES = Array("emg_0","emg_1","emg_2","emg_3","emg_4","emg_5","emg_6","emg_7","normalizedQuaternion_x","normalizedQuaternion_y","normalizedQuaternion_z","normalizedQuaternion_w","quaternion_x","quaternion_y","quaternion_z","quaternion_w","acceleratorMeter_x","acceleratorMeter_y","acceleratorMeter_z","gyro_x","gyro_y","gyro_z","normalizedAcceleratorMeter_x","normalizedAcceleratorMeter_y","normalizedAcceleratorMeter_z","normalizedGyro_x","normalizedGyro_y","normalizedGyro_z","eulerAngels_pitch","eulerAngels_roll","eulerAngels_yaw","normalizedEulerAngels_pitch","normalizedEulerAngels_roll","normalizedEulerAngels_yaw")
  val DATA_PATH = "D:/dev/code/myo-streaming/src/main/resources"
  val MODEL_PATH = "D:/dev/spark-output/crossvallidator-model"
  val DATA_SERVER_PORT = 5555
}