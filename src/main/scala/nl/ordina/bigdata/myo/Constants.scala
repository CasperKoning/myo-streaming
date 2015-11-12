package nl.ordina.bigdata.myo

object Constants {
  val FEATURES_UNAGGREGATED = Array(
    "emg_0", "emg_1", "emg_2", "emg_3", "emg_4", "emg_5", "emg_6", "emg_7",
    //    "normalizedQuaternion_x", "normalizedQuaternion_y", "normalizedQuaternion_z", "normalizedQuaternion_w",
    //    "quaternion_x", "quaternion_y", "quaternion_z", "quaternion_w",
    "acceleratorMeter_x", "acceleratorMeter_y", "acceleratorMeter_z",
    "gyro_x", "gyro_y", "gyro_z",
    "normalizedAcceleratorMeter_x", "normalizedAcceleratorMeter_y", "normalizedAcceleratorMeter_z",
    "normalizedGyro_x", "normalizedGyro_y", "normalizedGyro_z"
    //    "eulerAngels_pitch", "eulerAngels_roll", "eulerAngels_yaw",
    //    "normalizedEulerAngels_pitch", "normalizedEulerAngels_roll", "normalizedEulerAngels_yaw"
  )

  val FEATURES_AGGREGATED = Array(
    "min(emg_0)", "min(emg_1)", "min(emg_2)", "min(emg_3)", "min(emg_4)", "min(emg_5)", "min(emg_6)", "min(emg_7)",
    "avg(emg_0)", "avg(emg_1)", "avg(emg_2)", "avg(emg_3)", "avg(emg_4)", "avg(emg_5)", "avg(emg_6)", "avg(emg_7)",
    "max(emg_0)", "max(emg_1)", "max(emg_2)", "max(emg_3)", "max(emg_4)", "max(emg_5)", "max(emg_6)", "max(emg_7)",
    "min(normalizedQuaternion_x)", "min(normalizedQuaternion_y)", "min(normalizedQuaternion_z)", "min(normalizedQuaternion_w)",
    "avg(normalizedQuaternion_x)", "avg(normalizedQuaternion_y)", "avg(normalizedQuaternion_z)", "avg(normalizedQuaternion_w)",
    "max(normalizedQuaternion_x)", "max(normalizedQuaternion_y)", "max(normalizedQuaternion_z)", "max(normalizedQuaternion_w)",
    "min(quaternion_x)", "min(quaternion_y)", "min(quaternion_z)", "min(quaternion_w)",
    "avg(quaternion_x)", "avg(quaternion_y)", "avg(quaternion_z)", "avg(quaternion_w)",
    "max(quaternion_x)", "max(quaternion_y)", "max(quaternion_z)", "max(quaternion_w)",
    "min(acceleratorMeter_x)", "min(acceleratorMeter_y)", "min(acceleratorMeter_z)",
    "avg(acceleratorMeter_x)", "avg(acceleratorMeter_y)", "avg(acceleratorMeter_z)",
    "max(acceleratorMeter_x)", "max(acceleratorMeter_y)", "max(acceleratorMeter_z)",
    "min(normalizedAcceleratorMeter_x)", "min(normalizedAcceleratorMeter_y)", "min(normalizedAcceleratorMeter_z)",
    "avg(normalizedAcceleratorMeter_x)", "avg(normalizedAcceleratorMeter_y)", "avg(normalizedAcceleratorMeter_z)",
    "max(normalizedAcceleratorMeter_x)", "max(normalizedAcceleratorMeter_y)", "max(normalizedAcceleratorMeter_z)",
    "min(gyro_x)", "min(gyro_y)", "min(gyro_z)",
    "avg(gyro_x)", "avg(gyro_y)", "avg(gyro_z)",
    "max(gyro_x)", "max(gyro_y)", "max(gyro_z)",
    "min(normalizedGyro_x)", "min(normalizedGyro_y)", "min(normalizedGyro_z)",
    "avg(normalizedGyro_x)", "avg(normalizedGyro_y)", "avg(normalizedGyro_z)",
    "max(normalizedGyro_x)", "max(normalizedGyro_y)", "max(normalizedGyro_z)",
    "min(eulerAngels_pitch)", "min(eulerAngels_roll)", "min(eulerAngels_yaw)",
    "avg(eulerAngels_pitch)", "avg(eulerAngels_roll)", "avg(eulerAngels_yaw)",
    "max(eulerAngels_pitch)", "max(eulerAngels_roll)", "max(eulerAngels_yaw)",
    "min(normalizedEulerAngels_pitch)", "min(normalizedEulerAngels_roll)", "min(normalizedEulerAngels_yaw)",
    "avg(normalizedEulerAngels_pitch)", "avg(normalizedEulerAngels_roll)", "avg(normalizedEulerAngels_yaw)",
    "max(normalizedEulerAngels_pitch)", "max(normalizedEulerAngels_roll)", "max(normalizedEulerAngels_yaw)"
  )

  val DATA_PATH = "D:/dev/code/myo-streaming/src/main/resources"
  val MODEL_PATH = "D:/dev/spark-output/crossvallidator-model"
  val DATA_SERVER_PORT = 5555
  val DATA_OUTPUT_PORT = 5556
}