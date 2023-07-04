#include <utility>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getMostSubtype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include "Common/Exception.h"
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/assert_cast.h>
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Interpreters/Context_fwd.h"
#include <base/TypeLists.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionGroupSortedArray : public IFunction
{
public:
    static constexpr auto name = "groupSortedArray";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGroupSortedArray>(context); }
    explicit FunctionGroupSortedArray(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    ContextPtr context;

    /// Initially allocate a piece of memory for 64 elements. NOTE: This is just a guess.
    static constexpr size_t INITIAL_SIZE_DEGREE = 6;

    struct UnpackedArrays
    {
        size_t base_rows = 0;

        struct UnpackedArray
        {
            bool is_const = false;
            const NullMap * null_map = nullptr;
            const NullMap * overflow_mask = nullptr;
            const ColumnArray::ColumnOffsets::Container * offsets = nullptr;
            const IColumn * nested_column = nullptr;

        };

        std::vector<UnpackedArray> args;
        Columns column_holders;

        UnpackedArrays() = default;
    };

    /// Cast column to data_type removing nullable if data_type hasn't.
    /// It's expected that column can represent data_type after removing some NullMap's.
    ColumnPtr castRemoveNullable(const ColumnPtr & column, const DataTypePtr & data_type) const;

    struct CastArgumentsResult
    {
        ColumnsWithTypeAndName initial;
        ColumnsWithTypeAndName casted;
    };

    static CastArgumentsResult castColumns(const ColumnsWithTypeAndName & arguments,
                                           const DataTypePtr & return_type, const DataTypePtr & return_type_with_nulls);
    UnpackedArrays prepareArrays(const ColumnsWithTypeAndName & columns, ColumnsWithTypeAndName & initial_columns) const;

    template <typename Map, typename ColumnType, bool is_numeric_column>
    ColumnPtr executeOneParam(size_t length, const UnpackedArrays & array, MutableColumnPtr result_data_ptr)
    {
        auto args = array.args.size();
        auto rows = array.base_rows;

        bool all_nullable = true;

        std::vector<const ColumnType *> columns;
        columns.reserve(args);
        for (const auto & arg : array.args)
        {
            if constexpr (std::is_same_v<ColumnType, IColumn>)
                columns.push_back(arg.nested_column);
            else
                columns.push_back(checkAndGetColumn<ColumnType>(arg.nested_column));

            if (!columns.back())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected array type for function groupSortedArray");

            if (!arg.null_map)
                all_nullable = false;
        }
        auto & result_data = static_cast<ColumnType &>(*result_data_ptr);
        auto result_offsets_ptr = ColumnArray::ColumnOffsets::create(rows);
        auto & result_offsets = assert_cast<ColumnArray::ColumnOffsets &>(*result_offsets_ptr);
        auto null_map_column = ColumnUInt8::create();
        NullMap & null_map = assert_cast<ColumnUInt8 &>(*null_map_column).getData();

        Map map;
        Array res;
        std::vector<size_t> prev_off(args, 0);
        size_t result_offset = 0;

        map.clear();
        bool all_has_nullable = all_nullable;
                
        for (size_t arg_num = 0; arg_num < args; ++ arg_num) 
        {
            const auto & arg = array.args[arg_num];
            bool current_has_nullable = false;

            size_t off;
            off = (*arg.offsets)[0];
                    
            for (auto i : collections::range(prev_off[arg_num], off))
            {
                if (arg.null_map && (*arg.null_map)[i])
                    current_has_nullable = true;
                else if (!arg.overflow_mask || (*arg.overflow_mask)[i] == 0)
                {
                    typename Map::mapped_type * value = nullptr;

                    if constexpr (is_numeric_column)
                    {
                        value = &map[columns[arg_num]->getElement(i)];
                    }
                    res[i] = map[columns[arg_num]->getElement(i)];
                }
            }
            prev_off[arg_num] = off;
            if (arg.is_const)
                prev_off[arg_num] = 0;

            if (!current_has_nullable)
                all_has_nullable = false;
        }
        if (all_has_nullable)
        {
            ++result_offset;
            result_data.insertDefault();
            null_map.push_back(1);
        }
            
        for (size_t i = 0; i < res.size(); ++i)
        {
            for (size_t j = 0; j < res.size(); ++j)
            {
                std::swap(res[j], res[j+1]);
            }
        }
        for (size_t i = 0; i < res.size(); i++)
        {
            if (i == length)
                break;
            ++result_offset;
            if constexpr (is_numeric_column)
                result_data.insertValue(res[i]);
            
            if (all_nullable)
                null_map.push_back(0);
        }
        result_offsets.getElement(0) = result_offset;
        
        ColumnPtr result_column = std::move(result_data_ptr);
        if (all_nullable)
            result_column = ColumnNullable::create(result_column, std::move(null_map_column));
        return ColumnArray::create(result_column, std::move(result_offsets_ptr));
    }

    template <typename Map, typename ColumnType, bool is_numeric_column>
    ColumnPtr executeTwoParams(size_t length, const UnpackedArrays & arrays, MutableColumnPtr result_data_ptr)
    {
        auto args = arrays.args.size();
        auto rows = arrays.base_rows;
        if (rows != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect number of arguments");

        bool all_nullable = true;

        std::vector<const ColumnType *> columns;
        columns.reserve(args);
        for (const auto & arg : arrays.args)
        {
            if constexpr (std::is_same_v<ColumnType, IColumn>)
                columns.push_back(arg.nested_column);
            else
                columns.push_back(checkAndGetColumn<ColumnType>(arg.nested_column));

            if (!columns.back())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected array type for function arrayIntersect");

            if (!arg.null_map)
                all_nullable = false;
        }

        auto & result_data = static_cast<ColumnType &>(*result_data_ptr);
        auto result_offsets_ptr = ColumnArray::ColumnOffsets::create(rows);
        auto & result_offsets = assert_cast<ColumnArray::ColumnOffsets &>(*result_offsets_ptr);
        auto null_map_column = ColumnUInt8::create();
        NullMap & null_map = assert_cast<ColumnUInt8 &>(*null_map_column).getData();

        Map map;
        std::vector<size_t> prev_off(args, 0);
        Array res, arr1, arr2;        
        size_t result_offset = 0;
        for (size_t row = 0; row < rows; ++row)
        {
            map.clear();

            bool all_has_nullable = all_nullable;

            for (size_t arg_num = 0; arg_num < args; ++arg_num)
            {
                const auto & arg = arrays.args[arg_num];
                bool current_has_nullable = false;

                size_t off;
                // const array has only one row
                if (arg.is_const)
                    off = (*arg.offsets)[0];
                else
                    off = (*arg.offsets)[row];

                for (auto i : collections::range(prev_off[arg_num], off))
                {
                    if (arg.null_map && (*arg.null_map)[i])
                        current_has_nullable = true;
                    else if (!arg.overflow_mask || (*arg.overflow_mask)[i] == 0)
                    {
                        typename Map::mapped_type * value = nullptr;

                        if constexpr (is_numeric_column)
                        {
                            res[i] = &map[columns[arg_num]->getElement(i)];
                        }
                    }
                }

                prev_off[arg_num] = off;
                if (arg.is_const)
                    prev_off[arg_num] = 0;

                if (!current_has_nullable)
                    all_has_nullable = false;
            }
            for (auto i = 0; i < int(res.size()/2); ++i)
            {
                arr1[i] = res[i];
                arr2[i] = res[i + int(res.size() / 2)];
            }
            if (all_has_nullable)
            {
                ++result_offset;
                result_data.insertDefault();
                null_map.push_back(1);
            }

            for (const auto & pair : map)
            {
                if (pair.getMapped() == args)
                {
                    ++result_offset;
                    if constexpr (is_numeric_column)
                        result_data.insertValue(pair.getKey());
                    else if constexpr (std::is_same_v<ColumnType, ColumnString> || std::is_same_v<ColumnType, ColumnFixedString>)
                        result_data.insertData(pair.getKey().data, pair.getKey().size);
                    else
                        result_data.deserializeAndInsertFromArena(pair.getKey().data);

                    if (all_nullable)
                        null_map.push_back(0);
                }
            }
            result_offsets.getElement(row) = result_offset;
        }

        ColumnPtr result_column = std::move(result_data_ptr);
        if (all_nullable)
            result_column = ColumnNullable::create(result_column, std::move(null_map_column));
        return ColumnArray::create(result_column, std::move(result_offsets_ptr));
    }
};

REGISTER_FUNCTION(ArrayIntersect)
{
    factory.registerFunction<FunctionGroupSortedArray>();
}

}
