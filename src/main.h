/**
 * Утилита может фоново переносить записи
 * из одной таблицы в другую, если уникальный ключ называется 'id'
 * и расположен первым в структуре
 */


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <signal.h>
#include <fcntl.h>


#include <mysql/mysql.h>
#include "ini.h"

#define ERROR_VAL -1
#define OK_VAL 0

#define DEFAULT_PROGRAMM_NAME "db_replicator"
#define MAX_CONFIG_PARAM_LENGTH 100
#define MAX_QUERY_LENGTH 5000
#define MAX_INI_PATH_LENGTH 100

#define MAX_FIELDS_VALUES_LENGTH 2100
#define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0


typedef struct {

	char mysqlHost[MAX_CONFIG_PARAM_LENGTH];
	char mysqlPort[MAX_CONFIG_PARAM_LENGTH];
	char mysqlUser[MAX_CONFIG_PARAM_LENGTH];
	char mysqlPassword[MAX_CONFIG_PARAM_LENGTH];
	char mysqlDb[MAX_CONFIG_PARAM_LENGTH];
	char mysqlTable[MAX_CONFIG_PARAM_LENGTH];

	char mysqlFinalHost[MAX_CONFIG_PARAM_LENGTH];
	char mysqlFinalPort[MAX_CONFIG_PARAM_LENGTH];
	char mysqlFinalUser[MAX_CONFIG_PARAM_LENGTH];
	char mysqlFinalPassword[MAX_CONFIG_PARAM_LENGTH];
	char mysqlFinalDb[MAX_CONFIG_PARAM_LENGTH];
	char mysqlFinalTable[MAX_CONFIG_PARAM_LENGTH];

	char rowsCount[MAX_CONFIG_PARAM_LENGTH];
	char daemonName[MAX_CONFIG_PARAM_LENGTH];
	char waitTime[MAX_CONFIG_PARAM_LENGTH];
	char errorWaitTime[MAX_CONFIG_PARAM_LENGTH];

	char journalName[MAX_CONFIG_PARAM_LENGTH];
	char pidfilePath[MAX_CONFIG_PARAM_LENGTH];


} config;

int debug = 0;
int inProcess = 1;
int pidFilehandle = 0;

config conf;

int process();
int logMsg(int priority, const char *logMessage);
int readConfig(char *path);
static int configIniHandler(void *user, const char *section, const char *name, const char *value);
static void signalHendler(int signo);

int mysqlConnect(MYSQL *mysqlInitial, MYSQL *mysqlFinal);
int mysqlSTMTConnect(MYSQL_STMT *stmtGetRows, MYSQL_STMT *stmtDeleteRow, int *rowId);
int mysqlSTMTCloseConnection(MYSQL_STMT *stmtGetRows, MYSQL_STMT *stmtDeleteRow);
int mysqlCloseConnection(MYSQL *mysql);
int mysqlGetRows(MYSQL_STMT *stmtGetRows, MYSQL_STMT *stmtDeleteRow, MYSQL *mysqlFinal, int *rowId);
int deleteRow(MYSQL_STMT *stmtDeleteRow);
int insertToFinalDB (MYSQL *mysqlFinal, char *smsArchiveRow, int fieldsNumber, my_bool *isNull);


