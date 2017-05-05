package it.uniud.newbestsub.utils

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging.*

class BestSubsetFormatter : Formatter() {

    override fun format(record: LogRecord): String {
        val dateFormat = SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS")
        val builder = StringBuilder(1000)
        builder.append(formatMessage(record))
        builder.append(" - TIME: ").append(dateFormat.format(Date(record.millis)))
        builder.append("\n")
        return builder.toString()
    }

    override fun getHead(h: Handler?): String {
        return super.getHead(h)
    }

    override fun getTail(h: Handler?): String {
        return super.getTail(h)
    }

}
