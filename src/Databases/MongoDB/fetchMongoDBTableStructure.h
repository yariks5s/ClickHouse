#pragma once

#include "config.h"

#if USE_LIBPQXX
#include <Core/NamesAndTypes.h>
#include <Poco/MongoDB/Connection.h>

namespace DB
{

struct MongoDBTableStructure
{
    struct MongoDBAttribute
    {
        Int32 atttypid;
        Int32 atttypmod;
    };
    using Attributes = std::vector<MongoDBAttribute>;

    struct ColumnsInfo
    {
        NamesAndTypesList columns;
        Attributes attributes;
        ColumnsInfo(NamesAndTypesList && columns_, Attributes && attributes_) : columns(columns_), attributes(attributes_) {}
    };
    using ColumnsInfoPtr = std::shared_ptr<ColumnsInfo>;

    ColumnsInfoPtr physical_columns;
    ColumnsInfoPtr primary_key_columns;
    ColumnsInfoPtr replica_identity_columns;
};

using MongoDBTableStructurePtr = std::unique_ptr<MongoDBTableStructure>;

/// We need order for materialized version.
std::set<String> fetchPostgreSQLTablesList(Poco::MongoDB::Connection & connection, const String & mongodb_schema);

MongoDBTableStructure fetchMongoDBTableStructure(
    Poco::MongoDB::Connection & connection, const String & mongodb_table, const String & mongodb_schema, bool use_nulls = true);

template<typename T>
MongoDBTableStructure fetchMongoDBTableStructure(
    T & tx, const String & mongodb_table, const String & mongodb_schema, bool use_nulls = true,
    bool with_primary_key = false, bool with_replica_identity_index = false);

template<typename T>
std::set<String> fetchMongoDBTablesList(T & tx, const String & mongodb_schema);

}

#endif
