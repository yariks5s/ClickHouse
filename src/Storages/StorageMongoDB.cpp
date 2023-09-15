#include <Storages/StorageMongoDB.h>
#include <Storages/StorageMongoDBSocketFactory.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/MongoDB/Document.h>
#include <Poco/MongoDB/ObjectId.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/parseAddress.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Core/ExternalResultDescription.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Storages/ColumnsDescription.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/MongoDBSource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataTypeArray.h>

#define input_format_max_rows_to_read_for_schema_inference 100

namespace DB
{

using ValueType = ExternalResultDescription::ValueType;
using ObjectId = Poco::MongoDB::ObjectId;
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
    extern const int MONGODB_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int TYPE_MISMATCH;
}

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    const std::string & host_,
    uint16_t port_,
    const std::string & database_name_,
    const std::string & collection_name_,
    const std::string & username_,
    const std::string & password_,
    const std::string & options_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , database_name(database_name_)
    , collection_name(collection_name_)
    , username(username_)
    , password(password_)
    , uri("mongodb://" + host_ + ":" + std::to_string(port_) + "/" + database_name_ + "?" + options_)
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(*connection,  database_name, collection_name);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


void StorageMongoDB::connectIfNotConnected()
{
    std::lock_guard lock{connection_mutex};
    if (!connection)
    {
        StorageMongoDBSocketFactory factory;
        connection = std::make_shared<Poco::MongoDB::Connection>(uri, factory);
    }

    if (!authenticated)
    {
        Poco::URI poco_uri(uri);
        auto query_params = poco_uri.getQueryParameters();
        auto auth_source = std::find_if(query_params.begin(), query_params.end(),
                                        [&](const std::pair<std::string, std::string> & param) { return param.first == "authSource"; });
        auto auth_db = database_name;
        if (auth_source != query_params.end())
            auth_db = auth_source->second;

        if (!username.empty() && !password.empty())
        {
            Poco::MongoDB::Database poco_db(auth_db);
            if (!poco_db.authenticate(*connection, username, password, Poco::MongoDB::Database::AUTH_SCRAM_SHA1))
                throw Exception(ErrorCodes::MONGODB_CANNOT_AUTHENTICATE, "Cannot authenticate in MongoDB, incorrect user or password");
        }

        authenticated = true;
    }
}


class StorageMongoDBSink : public SinkToStorage
{
public:
    explicit StorageMongoDBSink(
        const std::string & collection_name_,
        const std::string & db_name_,
        const StorageMetadataPtr & metadata_snapshot_,
        std::shared_ptr<Poco::MongoDB::Connection> connection_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , collection_name(collection_name_)
        , db_name(db_name_)
        , metadata_snapshot{metadata_snapshot_}
        , connection(connection_)
        , is_wire_protocol_old(isMongoDBWireProtocolOld(*connection_))
    {
    }

    String getName() const override { return "StorageMongoDBSink"; }

    void consume(Chunk chunk) override
    {
        Poco::MongoDB::Database db(db_name);
        Poco::MongoDB::Document::Vector documents;

        auto block = getHeader().cloneWithColumns(chunk.detachColumns());

        size_t num_rows = block.rows();
        size_t num_cols = block.columns();

        const auto columns = block.getColumns();
        const auto data_types = block.getDataTypes();
        const auto data_names = block.getNames();

        documents.reserve(num_rows);

        for (const auto i : collections::range(0, num_rows))
        {
            Poco::MongoDB::Document::Ptr document = new Poco::MongoDB::Document();

            for (const auto j : collections::range(0, num_cols))
            {
                insertValueIntoMongoDB(*document, data_names[j], *data_types[j], *columns[j], i);
            }

            documents.push_back(std::move(document));
        }

        if (is_wire_protocol_old)
        {
            Poco::SharedPtr<Poco::MongoDB::InsertRequest> insert_request = db.createInsertRequest(collection_name);
            insert_request->documents() = std::move(documents);
            connection->sendRequest(*insert_request);
        }
        else
        {
            Poco::SharedPtr<Poco::MongoDB::OpMsgMessage> insert_request = db.createOpMsgMessage(collection_name);
            insert_request->setCommandName(Poco::MongoDB::OpMsgMessage::CMD_INSERT);
            insert_request->documents() = std::move(documents);
            connection->sendRequest(*insert_request);
        }
    }

private:

    void insertValueIntoMongoDB(
        Poco::MongoDB::Document & document,
        const std::string & name,
        const IDataType & data_type,
        const IColumn & column,
        size_t idx)
    {
        WhichDataType which(data_type);

        if (which.isArray())
        {
            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const ColumnArray::Offsets & offsets = column_array.getOffsets();

            size_t offset = offsets[idx - 1];
            size_t next_offset = offsets[idx];

            const IColumn & nested_column = column_array.getData();

            const auto * array_type = assert_cast<const DataTypeArray *>(&data_type);
            const DataTypePtr & nested_type = array_type->getNestedType();

            Poco::MongoDB::Array::Ptr array = new Poco::MongoDB::Array();
            for (size_t i = 0; i + offset < next_offset; ++i)
            {
                insertValueIntoMongoDB(*array, Poco::NumberFormatter::format(i), *nested_type, nested_column, i + offset);
            }

            document.add(name, array);
            return;
        }

        /// MongoDB does not support UInt64 type, so just cast it to Int64
        if (which.isNativeUInt())
            document.add(name, static_cast<Poco::Int64>(column.getUInt(idx)));
        else if (which.isNativeInt())
            document.add(name, static_cast<Poco::Int64>(column.getInt(idx)));
        else if (which.isFloat32())
            document.add(name, static_cast<Float64>(column.getFloat32(idx)));
        else if (which.isFloat64())
            document.add(name, static_cast<Float64>(column.getFloat64(idx)));
        else if (which.isDate())
            document.add(name, Poco::Timestamp(DateLUT::instance().fromDayNum(DayNum(column.getUInt(idx))) * 1000000));
        else if (which.isDateTime())
            document.add(name, Poco::Timestamp(column.getUInt(idx) * 1000000));
        else
        {
            WriteBufferFromOwnString ostr;
            data_type.getDefaultSerialization()->serializeText(column, idx, ostr, FormatSettings{});
            document.add(name, ostr.str());
        }
    }

    String collection_name;
    String db_name;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<Poco::MongoDB::Connection> connection;

    const bool is_wire_protocol_old;
};

DataTypePtr findType(ValueType type)
{
    switch (type)
        {
            case ValueType::vtUInt8:
                return std::shared_ptr<DataTypeUInt8>();
            case ValueType::vtUInt16:
                return std::shared_ptr<DataTypeUInt16>();
            case ValueType::vtUInt32:
                return std::shared_ptr<DataTypeUInt32>();
            case ValueType::vtUInt64:
                return std::shared_ptr<DataTypeUInt64>();
            case ValueType::vtInt8:
                return std::shared_ptr<DataTypeInt8>();
            case ValueType::vtInt16:
                return std::shared_ptr<DataTypeInt16>();
            case ValueType::vtInt32:
                return std::shared_ptr<DataTypeInt32>();
            case ValueType::vtInt64:
                return std::shared_ptr<DataTypeInt64>();
            case ValueType::vtFloat32:
                return std::shared_ptr<DataTypeFloat32>();
            case ValueType::vtFloat64:
                return std::shared_ptr<DataTypeFloat64>();
            case ValueType::vtEnum8:
                return std::shared_ptr<DataTypeEnum8>();
            case ValueType::vtEnum16:
                return std::shared_ptr<DataTypeEnum16>();
            case ValueType::vtString:
                return std::shared_ptr<DataTypeString>();
            case ValueType::vtDate:
                return std::shared_ptr<DataTypeDate>();
            case ValueType::vtDateTime:
                return std::shared_ptr<DataTypeDateTime>();
            case ValueType::vtUUID:
                return std::shared_ptr<DataTypeUUID>();
            case ValueType::vtArray:
                return std::shared_ptr<DataTypeArray>();
            default:
                return nullptr;
        }
}

ColumnsDescription getTableStructureFromData(Poco::MongoDB::Connection connection, std::string database_name, std::string collection_name)
{
    ColumnsDescription res = ColumnsDescription();
    ExternalResultDescription description;
    MongoDBCursor cursor(database_name, collection_name, description.sample_block, query, connection);
    // std::vector<std::string> list_of_types;
    MutableColumns columns(description.sample_block.columns());
    const size_t size = columns.size();

    size_t num_rows = 0;
    Array types;
    DataTypePtr element_type;
    auto documents = cursor.nextDocuments(connection);
    size_t lost_type_idx;

    for (auto document : documents)
    {
        bool is_all_processed = true;
        if (document->exists("ok") && document->exists("$err")
            && document->exists("code") && document->getInteger("ok") == 0)
        {
            auto code = document->getInteger("code");
            const Poco::MongoDB::Element::Ptr value = document->get("$err");
            auto message = static_cast<const Poco::MongoDB::ConcreteElement<String> &>(*value).value();
            throw Exception(ErrorCodes::MONGODB_ERROR, "Got error from MongoDB: {}, code: {}", message, code);
        }
        ++num_rows;
        for (const auto idx : collections::range(0, size))
        {
            const auto & name = description.sample_block.getByPosition(idx).name;

            bool exists_in_current_document = document->exists(name);
            if (!exists_in_current_document)
            {
                continue;
            }

            const Poco::MongoDB::Element::Ptr value = document->get(name);

            if (value.isNull() || value->type() == Poco::MongoDB::ElementTraits<Poco::MongoDB::NullValue>::TypeId)
            {
                element_type = findType(description.types[idx].first);
            }
            else
            {
                bool is_nullable = description.types[idx].second;
                if (is_nullable)
                {
                    ColumnNullable & column_nullable = assert_cast<ColumnNullable &>(*columns[idx]);
                    element_type = findType(description.types[idx].first);
                    column_nullable.getNullMapData().emplace_back(0);
                }
                else
                    element_type = findType(description.types[idx].first);
            }
            if (res.empty())            /// If we cannot find type for some row, right after that we go to another document and
            {                           /// continue from the point that we lost the type, and do it each time we lose the type
                lost_type_idx = idx;
                is_all_processed = false;
                break;
            }
            else if (!is_all_processed && lost_type_idx == idx)
                is_all_processed = true;

            if (is_all_processed)
            {
                ColumnDescription column(name, element_type);
                res.add(column);
            }
        }
        if (is_all_processed)
            break;

        // Allocate this data somewhere

        if (num_rows == input_format_max_rows_to_read_for_schema_inference)
            throw Exception(ErrorCodes::MONGODB_ERROR, "Cannot inference schema: too many attempts");
    }

    return res;
}

Pipe StorageMongoDB::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    connectIfNotConnected();

    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<MongoDBSource>(connection, database_name, collection_name, Poco::MongoDB::Document{}, sample_block, max_block_size));
}

SinkToStoragePtr StorageMongoDB::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr /* context */, bool /*async_insert*/)
{
    connectIfNotConnected();
    return std::make_shared<StorageMongoDBSink>(collection_name, database_name, metadata_snapshot, connection);
}

StorageMongoDB::Configuration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(
            *named_collection,
            ValidateKeysMultiset<MongoDBEqualKeysSet>{"host", "port", "user", "username", "password", "database", "db", "collection", "table"},
            {"options"});

        configuration.host = named_collection->getAny<String>({"host", "hostname"});
        configuration.port = static_cast<UInt16>(named_collection->get<UInt64>("port"));
        configuration.username = named_collection->getAny<String>({"user", "username"});
        configuration.password = named_collection->get<String>("password");
        configuration.database = named_collection->getAny<String>({"database", "db"});
        configuration.table = named_collection->getAny<String>({"collection", "table"});
        configuration.options = named_collection->getOrDefault<String>("options", "");
    }
    else
    {
        if (engine_args.size() < 5 || engine_args.size() > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage MongoDB requires from 5 to 6 parameters: "
                            "MongoDB('host:port', database, collection, 'user', 'password' [, 'options']).");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 27017 is the default MongoDB port.
        auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 27017);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        configuration.table = checkAndGetLiteralArgument<String>(engine_args[2], "table");
        configuration.username = checkAndGetLiteralArgument<String>(engine_args[3], "username");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[4], "password");

        if (engine_args.size() >= 6)
            configuration.options = checkAndGetLiteralArgument<String>(engine_args[5], "database");
    }

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}


void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageMongoDB::getConfiguration(args.engine_args, args.getLocalContext());

        return std::make_shared<StorageMongoDB>(
            args.table_id,
            configuration.host,
            configuration.port,
            configuration.database,
            configuration.table,
            configuration.username,
            configuration.password,
            configuration.options,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::MONGO,
    });
};

}
