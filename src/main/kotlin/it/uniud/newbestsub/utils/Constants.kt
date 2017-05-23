package it.uniud.newbestsub.utils

import java.text.SimpleDateFormat
import java.util.*

object Constants {
    val OUTPUT_PATH = "res/${SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())}/"
    val INPUT_PATH = "data/"
    val LOG_PATH = "log/${SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())}/"
}