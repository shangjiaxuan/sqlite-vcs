#define SQLITE_OMIT_LOAD_EXTENSION
#define SQLITE_DEBUG
#include "sqlite3.h"
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3;

#include <simdjson.h>

#include <cstdio>
#include <ctime>
#include <cstring>

#include <malloc.h>

class merger;

struct config {
public:
	const char* path;
	const char* source_path;
	const char* table;
	const char* source_table;
	const char* ID_name;
	const char* conflict_prefix;
	const char* conflict = nullptr;
	void init()
	{
		if (source_table_identifier) {
			free(source_table_identifier);
			source_table_identifier=nullptr;
		}
		if (!path || !source_path) return;
		if (!table) return;
		if (!ID_name) return;
		if (!conflict_prefix) conflict_prefix = "conflict";
		//init source table name
		{
			if (!source_table) {
				source_table = table;
			}
			int source_table_name_len = snprintf(nullptr, 0, "'source'.%s", source_table);
			source_table_identifier = (char*)malloc(source_table_name_len + 1);
			sprintf(source_table_identifier, "'source'.%s", source_table);
		}
	}
	void reverse(config& out)
	{
		if (&out == this) {
			std::swap(path, source_path);
			std::swap(table, source_table);
			return;
		}
		out.path = source_path;
		out.source_path = path;
		out.table = source_table;
		out.source_table = table;
		out.ID_name = ID_name;
		out.conflict_prefix = conflict_prefix;
		out.conflict = conflict;
	}
	config() = default;
	~config()
	{
		if (source_table_identifier) free(source_table_identifier);
	}
protected:
	friend merger;
	char* source_table_identifier = nullptr;
	//char* conflict = nullptr;
};

class merger {
	static void create_staging(sqlite3* db, const char* dest, const char* source, const char* ID_name, sqlite3_int64 id)
	{

		const char* format = "CREATE TABLE %s AS SELECT * FROM %s WHERE %s = %lld;";
		int len = snprintf(nullptr, 0, format, dest, source, ID_name, id);
		char* string = (char*)alloca(len + 1);
		sprintf(string, format, dest, source, ID_name, id);
		const char* tail;
		sqlite3_stmt* stmt;
		int err = sqlite3_prepare_v2(db, string, len + 1, &stmt, &tail);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
			return;
		}
		err = sqlite3_step(stmt);
		if (!(err == SQLITE_DONE || err == SQLITE_OK)) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
		}
		err = sqlite3_finalize(stmt);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
		}
	}
	static void insert_row(sqlite3* db, const char* dest, const char* source, const char* ID_name, sqlite3_int64 id)
	{
		const char* format = "INSERT INTO %s SELECT * FROM %s WHERE %s = %lld;";
		int len = snprintf(nullptr, 0, format, dest, source, ID_name, id);
		char* string = (char*)alloca(len + 1);
		sprintf(string, format, dest, source, ID_name, id);
		const char* tail;
		sqlite3_stmt* stmt;
		int err = sqlite3_prepare_v2(db, string, len + 1, &stmt, &tail);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
			return;
		}
		err = sqlite3_step(stmt);
		if (!(err == SQLITE_DONE || err == SQLITE_OK)) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
		}
		err = sqlite3_finalize(stmt);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
		}
	}
	// 1: exists
	// 0: not exist
	// -1: fail
	static int id_exists(sqlite3* db, const char* table, const char* ID, sqlite3_int64 id)
	{
		sqlite3_stmt* stmt;
		const char format[] = "SELECT * FROM %s WHERE %s = %lld;";
		int strlen = snprintf(nullptr, 0, format, table, ID, id);
		char* string = (char*)alloca(strlen + 1);
		sprintf(string, format, table, ID, id);
		int err = sqlite3_prepare_v2(db, string, strlen + 1, &stmt, nullptr);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
			return -1;
		}
		int res = sqlite3_step(stmt);
		int rtn;
		if (res == SQLITE_ROW) rtn = 1;
		else if (res == SQLITE_DONE) rtn = 0;
		else {
			rtn = -1;
			printf("%d:  %s\n", res, sqlite3_errstr(res));
		}
		err = sqlite3_finalize(stmt);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
		}
		return rtn;
	}
	//true: has conflict
	//false: no conflict
	static bool update(sqlite3* db, sqlite3_stmt* iterator, const char* dest, const char* source, const char* conflict_prefix, const char** m_conflict, const char* ID, bool check = false)
	{
		int found_index = -1;
		bool rtn = false;
		{
			int col_count = sqlite3_column_count(iterator);
			for (int index = 0; index < col_count; ++index) {
				if (strcmp(sqlite3_column_name(iterator, index), ID) == 0) {
					found_index = index;
					break;
				}
			}
		}
		if (found_index == -1) {
			printf("ID column not found\n");
			return true;
		}
		const char* conflict = nullptr;
		while (sqlite3_step(iterator) == SQLITE_ROW) {
			sqlite_int64 id = sqlite3_column_int64(iterator, found_index);
			int res = id_exists(db, dest, ID, id);
			if (res == 1) {
				rtn = true;
				if (!conflict) {
					if (check) {
						return true;
					}
					if (*m_conflict) {
						conflict = *m_conflict;
					}
					else {
						time_t now = time(0);
						tm* gmtm = gmtime(&now);
						const char fmt[] = "'%s-%04d%02d%02d-%02d:%02d:%02d'";
						int strlen = snprintf(nullptr, 0, fmt, conflict_prefix, (gmtm->tm_year) + 1900, (gmtm->tm_mon) + 1, gmtm->tm_mday, gmtm->tm_hour, gmtm->tm_min, gmtm->tm_sec);
						char* temp = (char*)malloc(strlen + 1);
						*m_conflict = temp;
						conflict = temp;
						sprintf(temp, fmt, conflict_prefix, (gmtm->tm_year) + 1900, (gmtm->tm_mon) + 1, gmtm->tm_mday, gmtm->tm_hour, gmtm->tm_min, gmtm->tm_sec);
						create_staging(db, conflict, source, ID, id);
					}
				}
				else {
					insert_row(db, conflict, source, ID, id);
				}
			}
			else if (res == 0) {
				insert_row(db, dest, source, ID, id);
			}
			else {
				printf("Failed to check existence\n");
				return true;
			}
		}
		return rtn;
	}

public:
	merger(config& cfg):m_cfg(cfg)
	{
		m_cfg.init();
		//open database
		int err = sqlite3_open_v2(m_cfg.path, &db, SQLITE_OPEN_READWRITE, nullptr);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
			return;
		}

		//attach input database
		{
			const char format[] = "ATTACH '%s' AS 'source';";
			int len = snprintf(nullptr, 0, format, m_cfg.source_path);
			char* string = (char*)alloca(len + 1);
			sprintf(string, format, m_cfg.source_path);
			char* errmsg;
			err = sqlite3_exec(db, string, nullptr, nullptr, &errmsg);
			if (err != SQLITE_OK) {
				printf("%d:  %s\n", err, errmsg);
				err = sqlite3_close_v2(db);
				db =  nullptr;
				if (err != SQLITE_OK) {
					printf("%d:  %s\n", err, sqlite3_errstr(err));
					return;
				}
			}
		}

		//verify structure here.
	}
	~merger()
	{
		if(free_conflict) free((char*)conflict);
		int err = sqlite3_close_v2(db);
		if (err != SQLITE_OK) {
			printf("%d\n", err);
		}
	}
	config& m_cfg;
	sqlite3* db;
	const char* conflict = nullptr;
	bool free_conflict = false;
	//true: has conflict
	//false: no conflict
	bool merge(bool check = false)
	{
		const char format[] = "SELECT * FROM 'source'.%s EXCEPT SELECT * FROM %s  ORDER BY %s;";
		int len = snprintf(nullptr, 0, format, m_cfg.table, m_cfg.table, m_cfg.ID_name);
		char* string = (char*)alloca(len+1);
		sprintf(string, format, m_cfg.table, m_cfg.table, m_cfg.ID_name);
		sqlite3_stmt* diff_iterator;
		int err = sqlite3_prepare_v2(db, string, len + 1, &diff_iterator, nullptr);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
			return true;
		}
		conflict = m_cfg.conflict;
		bool rtn = update(db, diff_iterator, m_cfg.table, m_cfg.source_table_identifier, m_cfg.conflict_prefix, &conflict, m_cfg.ID_name, check);
		if(conflict!=m_cfg.conflict) free_conflict = true;
		err = sqlite3_finalize(diff_iterator);
		if (err != SQLITE_OK) {
			printf("%d:  %s\n", err, sqlite3_errstr(err));
		}
		return rtn;
	}
};

class cfg_reader {
public:
	config cfg;
	cfg_reader(const char* cfg_path)
	{
		simdjson::dom::parser parser;
		simdjson::dom::element cfg_file = parser.load(cfg_path);
		
		cfg.path = strdup(cfg_file["master path"].get_c_str());
		cfg.source_path = strdup(cfg_file["commit path"].get_c_str());
		cfg.table = strdup(cfg_file["target table"].get_c_str());
		cfg.source_table = strdup(cfg_file["commit table"].get_c_str());
		cfg.ID_name = strdup(cfg_file["index column"].get_c_str());
		cfg.conflict_prefix = strdup(cfg_file["conflict prefix"].get_c_str());
	}
	~cfg_reader()
	{
		free((char*)cfg.path);
		free((char*)cfg.source_path);
		free((char*)cfg.table);
		free((char*)cfg.source_table);
		free((char*)cfg.ID_name);
		free((char*)cfg.conflict_prefix);
	}
};

constexpr int MODE_CHECK = 0;
constexpr int MODE_COMMIT = 1;
constexpr int MODE_PULL = 2;
constexpr int MODE_SYNC = 3;

void execute(config& m_cfg, int mode)
{
	switch (mode) {
		case MODE_CHECK:
			{
				merger mgr(m_cfg);
				if (!mgr.db) {
					printf("Failed to load database!\n");
					break;
				}
				if (mgr.merge(true)) {
					printf("Conflict detected, resolve conflict in table %s to continue.\n", mgr.conflict);
				}
			}
			break;
		case MODE_COMMIT:
			{
				merger mgr(m_cfg);
				if (!mgr.db) {
					printf("Failed to load database!\n");
					break;
				}
				if (mgr.merge()) {
					printf("Conflict detected, resolve conflict in table %s to continue.\n", mgr.conflict);
				}
			}
			break;
		case MODE_PULL:
			{
				config reversed;
				m_cfg.reverse(reversed);
				merger mgr(reversed);
				if (!mgr.db) {
					printf("Failed to load database!\n");
					break;
				}
				if (mgr.merge()) {
					printf("Conflict detected, resolve conflict in table %s to continue.\n", mgr.conflict);
				}
			}
			break;
		case MODE_SYNC:
			{
				bool do_commit = true;
				{
					config reversed;
					m_cfg.reverse(reversed);
					merger mgr(reversed);
					if (!mgr.db) {
						printf("Failed to load database!\n");
						break;
					}
					if (mgr.merge()) {
						do_commit = false;
						printf("Conflict detected, resolve conflict in table %s to continue.\n", mgr.conflict);
					}
				}
				if (do_commit) {
					merger mgr(m_cfg);
					if (!mgr.db) {
						printf("Failed to load database!\n");
						break;
					}
					if (mgr.merge()) {
						printf("Conflict detected, resolve conflict in table %s to continue.\n", mgr.conflict);
					}
				}
			}
			break;
	}
	printf("Done\n\nPress any key to exit...");
	getchar();
}

//means that no state is imbued, waiting for "-c" or "-m"
constexpr int ARG_NULL = 0;
constexpr int ARG_CONFIG = 1;
constexpr int ARG_MODE = 2;

void print_usage(const char* self, const char* invald_param)
{
	printf("Invalid parameter %s\n", invald_param);
	const char prompt[] = 
		"\tUsage:\n"
		"\t\t%s\n"
		"\t\t\t -c <config path>\n"
		"\t\t\t -m <mode=CHECK PULL COMMIT SYNC>\n";
	printf(prompt, self);
	printf("Press any key to exit...");
	getchar();
}

int main(int argc, char* argv[])
{
	const char* cfg_path = "config.json";
	int mode = MODE_SYNC;
	int cur_arg = 1;
	int arg_state = ARG_NULL;
	while (cur_arg < argc) {
		switch (arg_state) {
			case ARG_NULL:
				{
					int i = 0;
					while (isalpha(argv[cur_arg][i])) {
						argv[cur_arg][i] = tolower(argv[cur_arg][i]);
						++i;
					}
				}
				if (strcmp(argv[cur_arg], "-c")) {
					arg_state = ARG_CONFIG;
				}
				else if (strcmp(argv[cur_arg], "-m")) {
					arg_state = ARG_MODE;
				}
				else {
					print_usage(argv[0], argv[cur_arg]);
					exit(1);
				}
				break;
			case ARG_CONFIG:
				cfg_path = argv[cur_arg];
				arg_state = ARG_NULL;
				break;
			case ARG_MODE:
				{
					int i = 0;
					while (argv[cur_arg][i]) {
						argv[cur_arg][i] = toupper(argv[cur_arg][i]);
						++i;
					}
				}
				if (strcmp(argv[cur_arg], "CHECK")) {
					mode = MODE_CHECK;
				}
				else if (strcmp(argv[cur_arg], "PULL")) {
					mode = MODE_PULL;
				}
				else if (strcmp(argv[cur_arg], "COMMIT")) {
					mode = MODE_COMMIT;
				}
				else if (strcmp(argv[cur_arg], "SYNC")) {
					mode = MODE_SYNC;
				}
				else {
					print_usage(argv[0],argv[cur_arg]);
					exit(2);
				}
				arg_state = ARG_NULL;
				break;
		}
	}
	if (arg_state != ARG_NULL) {
		printf("Expecting parameter for %s\n",argv[argc-1]);
		printf("Press any key to exit...");
		getchar();
		exit(3);
	}
	cfg_reader reader(cfg_path);
	execute(reader.cfg, mode);
	return 0;
}
