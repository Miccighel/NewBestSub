package it.uniud.newbestsub.utils

import java.text.SimpleDateFormat
import java.util.*

object Constants {
    val INPUT_PATH = "data/"
    val OUTPUT_PATH = "res/${SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())}/"
    val LOG_PATH = "log/${SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())}/"
    val AVERAGE_EXPERIMENT_REPETITIONS = 100
}