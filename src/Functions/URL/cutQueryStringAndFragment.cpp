#include <Functions/FunctionFactory.h>
#include "queryStringAndFragment.h"
#include <Functions/FunctionStringToString.h>

namespace DB
{

struct NameCutQueryStringAndFragment { static constexpr auto name = "cutQueryStringAndFragment"; };
using FunctionCutQueryStringAndFragment = FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false>>, NameCutQueryStringAndFragment>;

REGISTER_FUNCTION(CutQueryStringAndFragment)
{
    factory.registerFunction<FunctionCutQueryStringAndFragment>();
}

}
