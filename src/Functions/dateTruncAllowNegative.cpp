#include <Functions/dateTruncBase.h>

namespace DB
{

class FunctionDateTruncAllowNegative : public FunctionDateTruncBase
{
public:
    static constexpr auto name = "dateTruncAllowNegative";

    explicit FunctionDateTruncAllowNegative(ContextPtr context_) : FunctionDateTruncBase(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionDateTruncAllowNegative>(context_); }
    
    String getName() const override { return name; }
    
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    ResultType decideReturnType(IntervalKind::Kind datepart_kind_) const override
    {
        if ((datepart_kind_ == IntervalKind::Kind::Year) || (datepart_kind_ == IntervalKind::Kind::Quarter)
            || (datepart_kind_ == IntervalKind::Kind::Month) || (datepart_kind_ == IntervalKind::Kind::Week))
            return ResultType::Date32;
        else
            return ResultType::DateTime64;
    }
};

REGISTER_FUNCTION(DateTruncAllowNegative)
{
    factory.registerFunction<FunctionDateTruncAllowNegative>(
            FunctionDocumentation{
            .description=R"(Same logic as dateTrunc but forced to return Date32 or DateTime64.)",
            .syntax=R"(dateTruncAllowNegative(unit, value[, timezone]))",
            .arguments={{"unit","The type of interval to truncate the result."}, {"value","Date/Date32 or DateTime/DateTime64"}, {"timezone","Timezone, for which the value will be applied"}},
            .examples={{"example","SELECT dateTruncAllowNegative('hour', toDateTime64('1960-03-03 12:10:10', 0));","1960-03-03 12:00:00"}}
        }
    );

    /// Compatibility alias.
    factory.registerAlias("DATE_TRUNC_ALLOW_NEGATIVE", "dateTruncAllowNegative", FunctionFactory::Case::Insensitive);
}

}
