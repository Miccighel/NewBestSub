package it.uniud.newbestsub.utils

import java.text.SimpleDateFormat
import java.util.*

object Constants {
    val INPUT_PATH = "data/"
    val OUTPUT_PATH = "res/"
    val LOG_PATH = "log/"
    //val OUTPUT_PATH = "res/${SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())}/"
    //val LOG_PATH = "log/${SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Date())}/"
    val TARGET_BEST = "Best"
    val TARGET_WORST = "Worst"
    val TARGET_AVERAGE = "Average"
    val TARGET_ALL = "All"
    val CORRELATION_PEARSON = "Pearson"
    val CORRELATION_KENDALL = "Kendall"
    val MAXIMUM_EXPANSION = 1200
}