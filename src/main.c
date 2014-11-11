/**
 * Утилита может фоново переносить записи
 * из одной таблицы в другую, если уникальный ключ таблицы называется 'id'
 * и расположен первым в структуре
 */


#include "main.h"

int main(int argc, char *argv[]){

	int rez = 0;
	char path[MAX_INI_PATH_LENGTH]= "config.ini";

	while ( (rez = getopt(argc,argv,"c:d")) != ERROR_VAL){
		switch (rez){
			case 'c':
				strcpy(path, optarg);
				break;
			case 'd':
				debug = 1;
				break;
			case '?':
				return ERROR_VAL;
				break;
		}
	}


	pid_t sid;

	if(debug == 0){

		int pid = fork();

		if (pid < 0) {
			logMsg(LOG_ERR, "Unable to start process");
			return ERROR_VAL;
		}

		if (pid > 0) {
			logMsg(LOG_ERR, "Process start");
			return ERROR_VAL;
		}

		sid = setsid();

		if (sid < 0) {
			logMsg(LOG_ERR, "Unable to set sid");
			return ERROR_VAL;
		}

//		if ((chdir("/")) < 0) {
//			logMsg(LOG_ERR, "Unable to change dir");
//			return ERROR_VAL;
//		}
		fclose(stdout);
		fclose(stderr);

	}
	if (signal(SIGINT, signalHendler) == SIG_ERR) {
		logMsg(LOG_ERR, "An error occurred while setting a signal handler");
		return ERROR_VAL;
	}

	memset(&conf, 0, sizeof(conf));

	if(readConfig(path)<0)
		return ERROR_VAL;


	pidFilehandle = open(conf.pidfilePath, O_RDWR | O_CREAT, 0600);

	if (pidFilehandle == -1 ){

		logMsg(LOG_INFO, "Process is already running");
		return ERROR_VAL;
	}

	if (lockf(pidFilehandle,F_TLOCK,0) == -1){

		logMsg(LOG_INFO, "Process is already running");
		return ERROR_VAL;

	}
    char str[10];
	sprintf(str,"%d\n",getpid());

	write(pidFilehandle, str, strlen(str));

	if(process() < 0)
		return ERROR_VAL;

	close(pidFilehandle);

	return OK_VAL;
}

/**
 *  Function starts main process
 */

int process()
{
	MYSQL mysqlInitial;
	MYSQL mysqlFinal;
	MYSQL_STMT *stmtGetRows;
	MYSQL_STMT *stmtDeleteRow;

	int rowId;

	while(inProcess == 1){

		if( mysqlConnect(&mysqlInitial, &mysqlFinal) > ERROR_VAL ){


			stmtGetRows = mysql_stmt_init(&mysqlInitial);
			stmtDeleteRow = mysql_stmt_init(&mysqlInitial);

			if( mysqlSTMTConnect(stmtGetRows, stmtDeleteRow, &rowId) > ERROR_VAL ){


				while(inProcess == 1){

					if(mysqlGetRows(stmtGetRows, stmtDeleteRow, &mysqlFinal, &rowId) < 0){

						break;
					}
//					usleep(atoi(conf.waitTime));

				}

			}
			mysqlSTMTCloseConnection(stmtGetRows, stmtDeleteRow);


		}
		mysqlCloseConnection(&mysqlInitial);
		mysqlCloseConnection(&mysqlFinal);

		if(inProcess){
			usleep(atoi(conf.errorWaitTime));
		}
	}

	return OK_VAL;

}

/**
 *  Function gets rows from mysql
 */

int mysqlGetRows(MYSQL_STMT *stmtGetRows, MYSQL_STMT *stmtDeleteRow, MYSQL *mysqlFinal, int *rowId)
{

	//Get mysql procedure result

	if(mysql_stmt_execute(stmtGetRows)){
		logMsg(LOG_ERR, mysql_stmt_error(stmtGetRows));
		return ERROR_VAL;
	}
	if (mysql_stmt_store_result(stmtGetRows)){
		logMsg(LOG_ERR, mysql_stmt_error(stmtGetRows));
		return ERROR_VAL;
	}

	MYSQL_RES* prepare_meta_result = mysql_stmt_result_metadata(stmtGetRows);
	if (!prepare_meta_result){
		logMsg(LOG_ERR, mysql_stmt_error(stmtGetRows));
		return ERROR_VAL;
	}

	//Decl number of rows and fields in result table
	const int rowNumber = mysql_stmt_num_rows(stmtGetRows);
	const int fieldsNumber = mysql_num_fields(prepare_meta_result);

    char archiveRow[MAX_FIELDS_VALUES_LENGTH * fieldsNumber];
    memset(archiveRow, 0 , sizeof(archiveRow));

    if(rowNumber > 0){
    	int resCount;

    	for(resCount = 0; resCount < rowNumber; resCount++){

			MYSQL_BIND result[fieldsNumber];
			my_bool isNull[fieldsNumber], error[fieldsNumber];
			unsigned long len[fieldsNumber];

			int fieldsCount;

			for(fieldsCount = 0; fieldsCount < fieldsNumber; fieldsCount++){

				result[fieldsCount].buffer_type = MYSQL_TYPE_VAR_STRING;
				result[fieldsCount].buffer =  (char *) &archiveRow[fieldsCount * MAX_FIELDS_VALUES_LENGTH];
				result[fieldsCount].buffer_length = MAX_FIELDS_VALUES_LENGTH;
				result[fieldsCount].is_null= &isNull[fieldsCount];
				result[fieldsCount].length= &len[fieldsCount];
				result[fieldsCount].error = &error[fieldsCount];

			}
			if(mysql_stmt_bind_result(stmtGetRows, result)){
				logMsg(LOG_ERR, mysql_stmt_error(stmtGetRows));
				return ERROR_VAL;
			}

			int status;

			status = mysql_stmt_fetch(stmtGetRows);

			if (status == 1 || status == MYSQL_NO_DATA)
				break;


			if(insertToFinalDB(mysqlFinal, archiveRow, fieldsNumber, isNull) == ERROR_VAL ){
				return ERROR_VAL;
			}
			*rowId = atoi(&archiveRow[0]);

			deleteRow(stmtDeleteRow);

    	}

    }
    if ( rowNumber == 0 ){
    	inProcess = 0;
	}
    mysql_free_result(prepare_meta_result);
    mysql_stmt_next_result(stmtGetRows);

	return OK_VAL;

}

int deleteRow(MYSQL_STMT *stmtDeleteRow)
{

	if(mysql_stmt_execute(stmtDeleteRow)){
		logMsg(LOG_ERR, mysql_stmt_error(stmtDeleteRow));
		return ERROR_VAL;
	}


	if (!(mysql_stmt_fetch(stmtDeleteRow))){
		logMsg(LOG_ERR, mysql_stmt_error(stmtDeleteRow));
		return ERROR_VAL;
	}

	return OK_VAL;
}


int insertToFinalDB (MYSQL *mysqlFinal, char *archiveRow, int fieldsNumber, my_bool *isNull)
{
	char query[MAX_QUERY_LENGTH];
	sprintf(query, "INSERT INTO %s VALUES (", conf.mysqlFinalTable);

	int fieldsCount;
	for(fieldsCount = 0; fieldsCount < fieldsNumber; fieldsCount++)	{

		if(fieldsCount != 0){

			sprintf(query, "%s ,", query);
		}
		if(isNull[fieldsCount] != 0){

			sprintf(query, "%s NULL ", query);

		}  else {
			char value[MAX_FIELDS_VALUES_LENGTH];
			int length = strlen(&archiveRow[fieldsCount*MAX_FIELDS_VALUES_LENGTH]);
			mysql_real_escape_string(mysqlFinal, value, &archiveRow[fieldsCount*MAX_FIELDS_VALUES_LENGTH], length);
			sprintf(query, "%s '%s' ", query, value);

		}
	}
	sprintf(query, "%s );", query);


	if(mysql_query(mysqlFinal, query)){
		logMsg(LOG_ERR, mysql_error(mysqlFinal));
		return ERROR_VAL;
	}

	return OK_VAL;
}


int mysqlConnect(MYSQL *mysqlInitial, MYSQL *mysqlFinal)
{
	mysql_init(mysqlInitial);
	mysql_init(mysqlFinal);

	if(!mysqlInitial || !mysqlFinal){
		logMsg(LOG_ERR, "MySQL init error\n");
		return ERROR_VAL;
	}

	if (!(mysql_real_connect(mysqlInitial, conf.mysqlHost,conf.mysqlUser, conf.mysqlPassword, conf.mysqlDb,
								atoi(conf.mysqlPort), NULL, CLIENT_MULTI_RESULTS))){
		logMsg(LOG_ERR, mysql_error(mysqlInitial));
		return ERROR_VAL;
	}
	if (!(mysql_real_connect(mysqlFinal, conf.mysqlFinalHost,conf.mysqlFinalUser, conf.mysqlFinalPassword, conf.mysqlFinalDb,
								atoi(conf.mysqlFinalPort), NULL, CLIENT_MULTI_RESULTS))){
		logMsg(LOG_ERR, mysql_error(mysqlFinal));
		return ERROR_VAL;
	}

	if (mysql_set_character_set(mysqlInitial, "utf8") || mysql_set_character_set(mysqlFinal, "utf8")) {
		logMsg(LOG_ERR, "Charset was not set");
		return ERROR_VAL;
	}
	return OK_VAL;

}
int mysqlSTMTConnect(MYSQL_STMT *stmtGetRows, MYSQL_STMT *stmtDeleteRow, int *rowId)
{

	if (!stmtGetRows || !stmtDeleteRow )
	{
		logMsg(LOG_ERR, "Could not initialize statement\n");
		return ERROR_VAL;
	}


	{
		char query[MAX_QUERY_LENGTH];

		sprintf(query, "SELECT * FROM %s LIMIT %d;", conf.mysqlTable, atoi(conf.rowsCount));

		if( mysql_stmt_prepare(stmtGetRows, query, MAX_QUERY_LENGTH) ){

			logMsg(LOG_ERR, mysql_stmt_error(stmtGetRows));
			return ERROR_VAL;
		}

	}

	{
		MYSQL_BIND rowDeleteParams[1];
		char deleteQuery[MAX_QUERY_LENGTH];

		sprintf(deleteQuery, "DELETE FROM %s WHERE id = ?;", conf.mysqlTable);

		if( mysql_stmt_prepare(stmtDeleteRow, deleteQuery, MAX_QUERY_LENGTH ) ){

			logMsg(LOG_ERR, mysql_stmt_error(stmtDeleteRow));
			return ERROR_VAL;
		}

		memset(rowDeleteParams, 0, sizeof (rowDeleteParams));

		rowDeleteParams[0].buffer_type = MYSQL_TYPE_LONG;
		rowDeleteParams[0].buffer = (char *)rowId;
		rowDeleteParams[0].length = 0;
		rowDeleteParams[0].is_null = 0;


		if ( mysql_stmt_bind_param(stmtDeleteRow, rowDeleteParams) ){

			logMsg(LOG_ERR, mysql_stmt_error(stmtDeleteRow));
			return ERROR_VAL;
		}
	}



	logMsg(LOG_DEBUG, "Mysql init ok");

	return OK_VAL;
}

int mysqlCloseConnection(MYSQL *mysql)
{
	if(mysql){
		mysql_close(mysql);
		logMsg(LOG_DEBUG, "Mysql close connection");
	}

	return OK_VAL;
}

int mysqlSTMTCloseConnection(MYSQL_STMT *stmtGetRows, MYSQL_STMT *stmtDeleteRow){

	if(stmtGetRows)
		mysql_stmt_close(stmtGetRows);

	if(stmtDeleteRow)
		mysql_stmt_close(stmtDeleteRow);

	logMsg(LOG_DEBUG, "Mysql STMT close connection");

	return OK_VAL;

}


/**
 *  Function reads config from file and writes to config structure
 */

int readConfig(char *path)
{

	if (ini_parse(path, configIniHandler, &conf) < 0 ) {
		logMsg(LOG_ERR, "Can't load config\n");
		return ERROR_VAL;
	}

	return OK_VAL;

}

static void signalHendler(int signo)
{

	switch(signo){

	case SIGTERM:
	case SIGABRT:
	case SIGINT:

		inProcess = 0;
		break;

	}
}

static int configIniHandler(void *user, const char *section, const char *name, const char *value)
{

	config *conf = (config*) user;

		//Настройки MySQL
    if (MATCH("mysql_config", "initial_mysql_host")) {
    	strcpy(conf->mysqlHost, value);

    } else if (MATCH("mysql_config", "initial_mysql_port")) {
    	strcpy(conf->mysqlPort, value);

    } else if (MATCH("mysql_config", "initial_mysql_user")) {
    	strcpy(conf->mysqlUser, value);

    } else if (MATCH("mysql_config", "initial_mysql_password")) {
    	strcpy(conf->mysqlPassword, value);

    } else if (MATCH("mysql_config", "initial_mysql_db")) {
    	strcpy(conf->mysqlDb, value);

    } else if (MATCH("mysql_config", "initial_mysql_table")) {
        strcpy(conf->mysqlTable, value);


    }else if (MATCH("mysql_config", "final_mysql_host")) {
		strcpy(conf->mysqlFinalHost, value);

	} else if (MATCH("mysql_config", "final_mysql_port")) {
		strcpy(conf->mysqlFinalPort, value);

	} else if (MATCH("mysql_config", "final_mysql_user")) {
		strcpy(conf->mysqlFinalUser, value);

	} else if (MATCH("mysql_config", "final_mysql_password")) {
		strcpy(conf->mysqlFinalPassword, value);

	} else if (MATCH("mysql_config", "final_mysql_db")) {
		strcpy(conf->mysqlFinalDb, value);

	} else if (MATCH("mysql_config", "final_mysql_table")) {
		strcpy(conf->mysqlFinalTable, value);


		//Настройки демона
	} else if (MATCH("daemon_config", "rows_count")) {
		strcpy(conf->rowsCount, value);

	} else if (MATCH("daemon_config", "wait_time")) {
		strcpy(conf->waitTime, value);

	} else if (MATCH("daemon_config", "error_wait_time")) {
		strcpy(conf->errorWaitTime, value);


	} else if (MATCH("daemon_config", "journal_name")) {
		strcpy(conf->journalName, value);

	} else if (MATCH("daemon_config", "pidfile_path")) {
		strcpy(conf->pidfilePath, value);

    } else {
        //return OK_VAL;  // unknown name
	}
    return OK_VAL;
}


int logMsg(int priority, const char *logMessage)
{
	if(!debug){
		if(priority != LOG_DEBUG){
			openlog(conf.journalName, 0, LOG_USER);
			syslog(priority, logMessage);
			closelog();
		}

	}else{

		printf(logMessage);
		printf("\n");

	}

	return OK_VAL;
}

