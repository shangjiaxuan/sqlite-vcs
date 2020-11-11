#define SQLITE_OMIT_LOAD_EXTENSION
#define SQLITE_DEBUG
#include "sqlite3.h"
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3;

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
		if (source_table_identifier) return;
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
		printf("%d\n", err);
		err = sqlite3_step(stmt);
		printf("%d\n", err);
		err = sqlite3_finalize(stmt);
		printf("%d\n", err);
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
		printf("%d\n", err);
		err = sqlite3_step(stmt);
		printf("%d\n", err);
		err = sqlite3_finalize(stmt);
		printf("%d\n", err);
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
		printf("%d\n", err);
		int res = sqlite3_step(stmt);
		int rtn;
		if (res == SQLITE_ROW) rtn = 1;
		else if (res == SQLITE_DONE) rtn = 0;
		else {
			rtn = -1;
			printf("%d\n", res);
		}
		err = sqlite3_finalize(stmt);
		printf("%d\n", err);
		return rtn;
	}
	//true: has conflict
	//false: no conflict
	static bool update(sqlite3* db, sqlite3_stmt* iterator, const char* dest, const char* source, const char* conflict_prefix, const char** m_conflict, const char* ID, bool check = false)
	{
		int found_index = -1;
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
				if (!conflict) {
					if (check) {
						return false;
					}
					if (*m_conflict) {
						conflict = *m_conflict;
					}
					else {
						time_t now = time(0);
						tm* gmtm = gmtime(&now);
						char* time = asctime(gmtm);
						const char fmt[] = "'%s %s'";
						int strlen = snprintf(nullptr, 0, fmt, conflict_prefix, time);
						char* temp = (char*)malloc(strlen + 1);
						*m_conflict = temp;
						conflict = temp;
						sprintf(temp, fmt, conflict_prefix, time);
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
				printf("%d", res);
			}
		}
		return true;
	}

public:
	merger(config& cfg):m_cfg(cfg)
	{
		m_cfg.init();
		//open database
		int err = sqlite3_open_v2(m_cfg.path, &db, SQLITE_OPEN_READWRITE, nullptr);
		printf("%d\n", err);

		//attach input database
		{
			const char format[] = "ATTACH %s AS 'source';";
			int len = snprintf(nullptr, 0, format, m_cfg.source_path);
			char* string = (char*)alloca(len + 1);
			sprintf(string, format, m_cfg.source_path);
			char* errmsg;
			err = sqlite3_exec(db, "ATTACH 'C:/Users/shang/OneDrive/Desktop/mega-mira.sqlte3.db' AS 'source'", nullptr, nullptr, &errmsg);
			printf("%d\t%s\n", err, errmsg);
		}

		//verify structure here.
	}
	~merger()
	{
		if(free_conflict) free((char*)conflict);
		int err = sqlite3_close_v2(db);
		printf("%d\n", err);
	}
	config& m_cfg;
	sqlite3* db;
	const char* conflict = nullptr;
	bool free_conflict = false;
	//true: has conflict
	//false: no conflict
	bool merge(bool check = false)
	{
		const char format[] = "SELECT * FROM 'source'.%s EXCEPT SELECT * FROM %s  ORDER BY %s";
		int len = snprintf(nullptr, 0, format, m_cfg.table, m_cfg.table, m_cfg.ID_name);
		char* string = (char*)alloca(len+1);
		sprintf(string, format, m_cfg.table, m_cfg.table, m_cfg.ID_name);
		sqlite3_stmt* diff_iterator;
		int err = sqlite3_prepare_v2(db, string, len + 1, &diff_iterator, nullptr);
		printf("%d\n", err);
		conflict = m_cfg.conflict;
		bool rtn = update(db, diff_iterator, m_cfg.table, m_cfg.source_table_identifier, m_cfg.conflict_prefix, &conflict, m_cfg.ID_name, check);
		if(conflict!=m_cfg.conflict) free_conflict = true;
		err = sqlite3_finalize(diff_iterator);
		printf("%d\n", err);
		return rtn;
	}
};

class cfg_reader {

};

int main()
{
	config cfg;
	cfg.path = "C:\\Users\\shang\\OneDrive\\Desktop\\test.sqlite3";
	cfg.source_path = "C:/Users/shang/OneDrive/Desktop/mega-mira.sqlte3.db";
	cfg.table = "Card";
	cfg.source_table = "Card";
	cfg.ID_name = "ID";
	cfg.conflict_prefix = "conflict";
	{
		merger mgr(cfg);
		if (mgr.merge()) {
			printf("conflict detected!\n");
		}
	}
	return 0;
}
