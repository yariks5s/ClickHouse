#include <Functions/dateTruncBase.h>

namespace DB
{

class FunctionDateTrunc : public FunctionDateTruncBase
{
public:
    static constexpr auto name = "dateTrunc";

    explicit FunctionDateTrunc(ContextPtr context_) : FunctionDateTruncBase(context_) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionDateTrunc>(context_); }
    
    String getName() const override { return name; }
    
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    ResultType decideReturnType(IntervalKind::Kind datepart_kind_) const override
    {
        if ((datepart_kind_ == IntervalKind::Kind::Year) || (datepart_kind_ == IntervalKind::Kind::Quarter)
            || (datepart_kind_ == IntervalKind::Kind::Month) || (datepart_kind_ == IntervalKind::Kind::Week))
            return ResultType::Date;
        else if ((datepart_kind_ == IntervalKind::Kind::Day) || (datepart_kind_ == IntervalKind::Kind::Hour)
                || (datepart_kind_ == IntervalKind::Kind::Minute) || (datepart_kind_ == IntervalKind::Kind::Second))
            return ResultType::DateTime;
        else
            return ResultType::DateTime64;
    }

};

REGISTER_FUNCTION(DateTrunc)
{
    factory.registerFunction<FunctionDateTrunc>();

    /// Compatibility alias.
    factory.registerAlias("DATE_TRUNC", "dateTrunc", FunctionFactory::Case::Insensitive);
}

}
