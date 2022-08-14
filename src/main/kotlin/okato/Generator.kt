package okato

import db.OkatoPlacesSynchronizer
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.Charset

class Generator(
    private val inputCsvFilePath: String,
    private val databaseUrl: String,
    private val databaseLogin: String,
    private val databasePassword: String
) {
    /**
     * Обрабатывает CSV-файл от ОКАТО и добавляет в БД данные, учитывая иерархию населенных пунктов.
     * Файлы от ОКАТО используют кодировку win-1251, это нужно учитывать при чтении данных.
     */
    fun generate() {
        val synchronizer = OkatoPlacesSynchronizer(
            databaseUrl,
            databaseLogin,
            databasePassword
        )
        val csvInputStream: InputStream = FileInputStream(File(inputCsvFilePath))
        val csvReader = BufferedReader(InputStreamReader(csvInputStream, Charset.forName("windows-1251")))
        try {
            val csvFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT)
                .setIgnoreSurroundingSpaces(true)
                .setDelimiter(';')
                .setQuote('"')
                .build()
            val csvParser: CSVParser = csvFormat.parse(csvReader)

            for (record: CSVRecord in csvParser) {
                if (record.recordNumber % 200 == 0L) {
                    println("Parsed rows: ${record.recordNumber}")
                }
                if (synchronizer.skipRecord(record)) {
                    continue
                }
                synchronizer.saveRecord(record)
            }
        } finally {
            csvReader.close()
        }
    }
}