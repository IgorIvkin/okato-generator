package db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.csv.CSVRecord
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction

/**
 * Вспомогательный DSL-объект для управления таблицей с населенными пунктами из БД.
 * Работает на основе библиотеки Exposed.
 */
object Places : Table() {
    val id = long(name = "id").autoIncrement()
    val title = varchar(name = "title", length = 255)
    val titleWithPronunciation = varchar(name = "title_with_pronunciation", length = 255)
    val countryId = varchar(name = "country_id", length = 2).default(OkatoPlacesSynchronizer.DEFAULT_COUNTRY)
    val parentPlaceId = long(name ="parent_place_id").nullable()
    val okatoCode = varchar(name = "okato_code", length = 255).nullable()

    // Определяем первичный ключ и задаём точное название таблицы
    override val primaryKey = PrimaryKey(id)
    override val tableName = "places"
}

class OkatoPlacesSynchronizer(
    private val databaseUrl: String,
    private val databaseLogin: String,
    private val databasePassword: String
) {

    companion object {
        /**
         * Номер поля с идентификатором региона.
         */
        const val REGION = 0

        /**
         * Номер поля с идентификатором района.
         */
        const val AREA = 1

        /**
         * Номер поля с идентификатором населенного пункта.
         */
        const val PLACE = 2

        /**
         * Номер поля с идентификатором района в населенном пункте.
         */
        const val DISTRICT = 3

        /**
         * Номер поля с названием места.
         */
        const val TERRITORY_NAME = 5

        /**
         * Страна по умолчанию.
         */
        const val DEFAULT_COUNTRY = "RU"
    }

    private val hikariConfig: HikariConfig = HikariConfig().apply {
        jdbcUrl = databaseUrl
        driverClassName = "org.postgresql.Driver"
        username = databaseLogin
        password = databasePassword
        maximumPoolSize = 20
    }

    private val dataSource = HikariDataSource(hikariConfig)

    /**
     * Словарь-кеш для хранения данных об уже добавленных местах.
     * Ключами являются коды ОКАТО, а значениями - идентификаторы из базы данных.
     */
    private val storedPlaces = mutableMapOf<String, Long>()

    /**
     * Предзаготовленный массив с согласными.
     */
    private val consonants = arrayOf(
        'б', 'в', 'г', 'д', 'ж', 'з', 'к', 'л', 'м', 'н', 'п', 'р', 'с',
        'т', 'ф', 'х', 'ц', 'ч', 'ш', 'щ'
    )

    /**
     * Обрабатывает очередную запись из CSV-файла ОКАТО и сохраняет ее в БД.
     * @param record CSV-запись из файла ОКАТО
     */
    fun saveRecord(record: CSVRecord) {
        val regionId = record.get(REGION)
        val areaId = record.get(AREA)
        val placeId = record.get(PLACE)
        val districtId = record.get(DISTRICT)
        val placeTitle = stripTitle(record.get(TERRITORY_NAME))
        val okatoId = buildOkatoId(regionId, areaId, placeId, districtId).replace("-000", "")
        val parentOkatoId = okatoId.replaceAfterLast("-", "").dropLastWhile { it == '-' }
        val parentId: Long? = storedPlaces[parentOkatoId]

        Database.connect(dataSource)
        transaction {
            val newlyAddedId: Long = Places.insert {
                it[title] = placeTitle
                it[okatoCode] = okatoId
                it[parentPlaceId] = parentId
                it[titleWithPronunciation] = buildPronunciation(placeTitle)
                it[countryId] = DEFAULT_COUNTRY
            } get Places.id
            storedPlaces.put(okatoId, newlyAddedId)
        }
    }

    /**
     * Определяет, пропускать ли запись из CSV-файла ОКАТО.
     * Пропускаются записи, в которых содержатся описательные вещи, и которые не являются населенными
     * пунктами, например, есть записи, в которых указано "Сельские населенные пункты". Такая запись
     * является чем-то вроде заголовка для последующих записей и не несёт ценности для базы.
     * @return true, если запись нужно пропустить
     */
    fun skipRecord(record: CSVRecord): Boolean {
        val title: String = record.get(TERRITORY_NAME)
        if (
            title.contains("Сельские населенные пункты")
            || title.contains("Объекты")
            || title.contains("Города районного значения")
            || title.contains("Города областного значения")
            || title.contains("Города краевого значения")
            || title.contains("Населенные пункты")
            || title.contains("Города, находящиеся в границах")
            || title.contains("Поселки городского типа")
            || title.contains("Административные округа")
            || title.contains("Районы")
            || title.contains("Сельсоветы")
        ) {
            return true
        }
        return false
    }

    private fun buildPronunciation(title: String): String {
        val nonPrecisedTitle = "в " + title.split(" ")
            .map { buildPronunciationOnWord(it) }
            .joinToString(" ")
        return preciseTitle(nonPrecisedTitle)
    }

    private fun buildPronunciationOnWord(title: String): String {
        return if (title == "км"
            || title == "им"
            || title == "Им"
            || title == "Коми"
            || title == "Марий"
            || title == "Эл"
            || title == "Ингушетия"
            || title == "Мордовия"
            || title == "Крым"
            || title == "Адыгея"
            || title == "Бурятия"
            || title == "Алтай"
            || title == "Калмыкия"
            || title == "Хакасия"
            || title == "Чувашия"
            || title == "Карелия"
            || title == "Центорой"
        ) {
            title
        } else if (title.endsWith("ая")) {
            title.replaceAfterLastWithLast("ая", "ой")
        } else if (title.endsWith("ий")) {
            title.replaceAfterLastWithLast("ий", "ом")
        } else if (title.endsWith("ия")) {
            title.replaceAfterLastWithLast("ия", "ии")
        } else if (title.endsWith("ай")) {
            title.replaceAfterLastWithLast("ай", "ае")
        } else if (title.endsWith("ый")) {
            title.replaceAfterLastWithLast("ый", "ом")
        } else if (title.endsWith("ое")) {
            title.replaceAfterLastWithLast("ое", "ом")
        } else if (title.endsWith("ой")) {
            title.replaceAfterLastWithLast("ой", "ом")
        } else if (title.endsWith("ья")) {
            title.replaceAfterLastWithLast("ья", "ье")
        } else if (title.endsWith("ль")) {
            title.replaceAfterLastWithLast("ль", "ле")
        } else if (title.endsWith("ь")) {
            title.replaceAfterLastWithLast("ь", "и")
        } else if (title.endsWith("а")) {
            title.replaceAfterLastWithLast("а", "е")
        } else if (title.endsWith("и")) {
            title.replaceAfterLastWithLast("и", "ах")
        } else if (title.endsWith("ы")) {
            title.replaceAfterLastWithLast("ы", "ах")
        } else if (title.last() in consonants) {
            title + "е"
        } else {
            title
        }


    }

    private fun preciseTitle(title: String): String {
        return title
            .replace("в Владимире", "во Владимире")
            .replace("ск-на-", "ске-на-")
            .replace("в Республике Северной Осетия-Алании", "в Республике Северная Осетия-Алания")
    }

    private fun stripTitle(title: String): String {
        return if (title.startsWith("п ")) {
            title.replaceFirst("п ", "")
        } else if (title.startsWith("д ")) {
            title.replaceFirst("д ", "")
        } else if (title.startsWith("с ")) {
            title.replaceFirst("с ", "")
        } else if (title.startsWith("г ")) {
            title.replaceFirst("г ", "")
        } else if (title.startsWith("ст ")) {
            title.replaceFirst("ст ", "")
        } else if (title.contains("Город Москва столица Российской Федерации")) {
            "Москва"
        } else if (title.contains("Санкт-Петербург город федерального значения")) {
            "Санкт-Петербург"
        } else if (title.contains("Город федерального значения Севастопол")) {
            "Севастополь"
        } else {
            title
        }
    }

    private fun buildOkatoId(regionId: String, areaId: String, placeId: String, districtId: String): String {
        return "$regionId-$areaId-$placeId-$districtId"
    }

    private fun String.replaceAfterLastWithLast(
        delimiter: String,
        replacement: String,
        missingDelimiterValue: String = this
    ): String {
        val index = lastIndexOf(delimiter)
        return if (index == -1) missingDelimiterValue else replaceRange(index, length, replacement)
    }
}