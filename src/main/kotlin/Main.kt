import okato.Generator

fun main(args: Array<String>) {
    println("OKATO generator started")

    // Если задан аргумент запуска программы, то используем первый переданный аргумент в качестве пути
    // к CSV-файлу с данными ОКАТО
    val inputCsvFilePath = if (args.isNotEmpty()) {
        args[0]
    } else {
        "d:/dev/JavaProjects/Family/realestate-projects/okato/db/data-20220701-structure-20140709.csv"
    }

    // Если заданы аргументы, то второй из них будет считаться URL в формате JDBC
    val databaseUrl = if (args.size > 1) {
        args[1]
    } else {
        "jdbc:postgresql://localhost:5432/realestate_db"
    }

    // Если заданы аргументы, то третий из них будет считаться логином для подключения к БД
    val databaseLogin = if (args.size > 2) {
        args[2]
    } else {
        "realestate_user"
    }

    // Если заданы аргументы, то второй из них будет считаться паролем для подключения к БД
    val databasePassword = if (args.size > 3) {
        args[3]
    } else {
        "realestate_pwd"
    }

    Generator(
        inputCsvFilePath = inputCsvFilePath,
        databaseUrl = databaseUrl,
        databaseLogin = databaseLogin,
        databasePassword = databasePassword
    ).generate()

    println("OKATO generator finished")
}