// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include <memory>
#include <random>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exprs/binary_predicate.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/function_context.h"
#include "exprs/json_functions.h"
#include "exprs/literal.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

constexpr size_t kChunkRows = 4096;

const std::vector<std::string> kPaths{"$.data.operation.pos.terminal.type", "$.data.operation.merchant.terminal.id",
                                      "$.data.operation.reference.ids.rrn"};

// A document shaped like solar_operations.original_payload, about 7.6 KB. `selected` decides whether the
// first path holds "ATM".
std::string payload(std::mt19937_64& random, bool selected) {
    auto uuid = [&random]() {
        static const char* hex = "0123456789abcdef";
        std::string s;
        for (int i = 0; i < 32; ++i) {
            if (i == 8 || i == 12 || i == 16 || i == 20) s += '-';
            s += hex[random() % 16];
        }
        return s;
    };
    std::string s = "{\"id\":\"" + uuid() +
                    "\",\"meta\":{\"source\":\"solar\",\"version\":3,\"created\":\"2026-10-14T12:00:00.000+05:00\"},";
    s += "\"items\":[";
    for (int i = 0; i < 38; ++i) {
        s += (i ? "," : "");
        s += "{\"k\":" + std::to_string(i) + ",\"name\":\"\\u041e\\u043f\\u043b\\u0430\\u0442\\u0430 \\\"" +
             std::to_string(random() % 1000) + "\\\"\",\"amount\":" + std::to_string(random() % 100000) + "." +
             std::to_string(random() % 100) + ",\"u\":\"" + uuid() + "\",\"flags\":[true,false,null],\"note\":\"" +
             std::string(40 + random() % 40, 'x') + "\"}";
    }
    s += "],\"data\":{\"operation\":{";
    s += "\"pos\":{\"terminal\":{\"type\":\"" + std::string(selected ? "ATM" : "POS") +
         "\",\"mode\":" + std::to_string(random() % 100) + "}},";
    s += "\"merchant\":{\"terminal\":{\"id\":\"A0400" + std::to_string(random() % 1000) + "\",\"mcc\":5411}},";
    s += "\"reference\":{\"ids\":{\"rrn\":\"" + std::to_string(random()) +
         "\",\"stan\":" + std::to_string(random() % 1000000) + "}}";
    s += "}}}";
    return s;
}

// A chunk of documents in which every `one_in`-th document holds "ATM" on the first path.
ColumnPtr documents(int one_in) {
    std::mt19937_64 random(42);
    auto column = BinaryColumn::create();
    for (size_t row = 0; row < kChunkRows; ++row) {
        column->append(payload(random, row % one_in == 0));
    }
    return column;
}

ColumnPtr const_text(const std::string& value) {
    auto data = BinaryColumn::create();
    data->append(value);
    return ConstColumn::create(std::move(data), kChunkRows);
}

using Kernel = StatusOr<ColumnPtr> (*)(FunctionContext*, const Columns&);
using StateFn = Status (*)(FunctionContext*, FunctionContext::FunctionStateScope);

// One call of a function with its prepared state.
class Call {
public:
    Call(Kernel kernel, StateFn prepare, StateFn close, Columns columns)
            : _kernel(kernel),
              _close(close),
              _columns(std::move(columns)),
              _ctx(FunctionContext::create_test_context()) {
        _ctx->set_constant_columns(_columns);
        if (prepare != nullptr) {
            CHECK(prepare(_ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        }
    }
    ~Call() {
        if (_close != nullptr) {
            CHECK(_close(_ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        }
    }
    ColumnPtr run(const ColumnPtr& input) {
        _columns[0] = input;
        return _kernel(_ctx.get(), _columns).value();
    }

private:
    Kernel _kernel;
    StateFn _close;
    Columns _columns;
    std::unique_ptr<FunctionContext> _ctx;
};

void finish(benchmark::State& state, const ColumnPtr& docs) {
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kChunkRows));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(docs->byte_size()));
}

// parse_json(v) followed by get_json_string over JSON: the reference semantics.
void BM_vpack_reference(benchmark::State& state) {
    const ColumnPtr docs = documents(1);
    Call get(JsonFunctions::get_native_json_string, JsonFunctions::native_json_path_prepare,
             JsonFunctions::native_json_path_close, {nullptr, const_text(kPaths[0])});
    std::unique_ptr<FunctionContext> parse_ctx(FunctionContext::create_test_context());
    for (auto _ : state) {
        ColumnPtr json = JsonFunctions::parse_json(parse_ctx.get(), {docs}).value();
        benchmark::DoNotOptimize(get.run(json));
    }
    finish(state, docs);
}

void BM_extract_one_path(benchmark::State& state) {
    const ColumnPtr docs = documents(1);
    Call get(JsonFunctions::get_json_string, JsonFunctions::native_json_path_prepare,
             JsonFunctions::native_json_path_close, {nullptr, const_text(kPaths[0])});
    for (auto _ : state) {
        benchmark::DoNotOptimize(get.run(docs));
    }
    finish(state, docs);
}

void BM_extract_three_paths(benchmark::State& state) {
    const ColumnPtr docs = documents(1);
    Call extract(JsonFunctions::json_extract_paths, JsonFunctions::json_extract_paths_prepare,
                 JsonFunctions::json_extract_paths_close,
                 {nullptr, const_text(kPaths[0]), const_text(kPaths[1]), const_text(kPaths[2])});
    for (auto _ : state) {
        benchmark::DoNotOptimize(extract.run(docs));
    }
    finish(state, docs);
}

// Conjuncts of a scan over one JSON column: X0 = 'ATM', X1 != '' and X2 != ''. Grouped, Xi is
// get_json_string(E, '$.i') and every conjunct has its own E = json_extract_paths(v, p0, p1, p2); otherwise Xi is
// get_json_string(v, pi).
class Conjuncts {
public:
    static constexpr SlotId kSlot = 1;

    Conjuncts(size_t count, bool grouped) {
        auto value = [&](int i) {
            return grouped ? get_json_string(extract(), ("$." + std::to_string(i)).c_str(), kJsonGetJsonString)
                           : get_json_string(column(), kPaths[i].c_str(), kTextGetJsonString);
        };
        std::vector<Expr*> roots{compare(TExprOpcode::EQ, value(0), text("ATM")),
                                 compare(TExprOpcode::NE, value(1), text("")),
                                 compare(TExprOpcode::NE, value(2), text(""))};
        roots.resize(count);
        for (auto* root : roots) {
            auto* context = _pool.add(new ExprContext(root));
            CHECK(context->prepare(&_state).ok());
            CHECK(context->open(&_state).ok());
            _contexts.push_back(context);
        }
    }
    ~Conjuncts() {
        for (auto* context : _contexts) {
            context->close(&_state);
        }
    }

    size_t with_cache(Chunk* chunk) {
        Filter filter(chunk->num_rows(), 1);
        return ExecNode::eval_conjuncts_into_filter(_contexts, chunk, &filter).value();
    }

    // Every conjunct computes its expression on the whole chunk, as the column expression predicates of the
    // storage layer do.
    size_t without_cache(Chunk* chunk) {
        Filter filter(chunk->num_rows(), 1);
        for (auto* context : _contexts) {
            ColumnHelper::merge_two_filters(context->evaluate(chunk).value(), &filter);
        }
        return SIMD::count_nonzero(filter);
    }

private:
    static constexpr int64_t kTextGetJsonString = 110002;
    static constexpr int64_t kJsonGetJsonString = 110014;

    static TypeDescriptor varchar_type() { return TypeDescriptor::create_varchar_type(1 << 20); }

    Expr* column() { return _pool.add(new ColumnRef(varchar_type(), kSlot)); }

    Expr* text(const std::string& value) {
        return _pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(value), 1),
                                               varchar_type()));
    }

    Expr* call(const char* name, int64_t fid, const TypeDescriptor& result_type, const std::vector<Expr*>& args) {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_function_name(name);
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_fid(fid);
        fn.__set_has_var_args(false);
        std::vector<TTypeDesc> arg_types;
        for (const auto* arg : args) {
            arg_types.push_back(arg->type().to_thrift());
        }
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(result_type.to_thrift());
        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_num_children(args.size());
        node.__set_is_nullable(true);
        node.__set_type(result_type.to_thrift());
        node.__set_fn(fn);
        auto* expr = _pool.add(new VectorizedFunctionCallExpr(node));
        for (auto* arg : args) {
            expr->add_child(arg);
        }
        return expr;
    }

    Expr* extract() {
        std::vector<Expr*> args{column()};
        for (const auto& path : kPaths) {
            args.push_back(text(path));
        }
        return call("json_extract_paths", 1100108, TypeDescriptor::create_json_type(), args);
    }

    Expr* get_json_string(Expr* document, const char* path, int64_t fid) {
        return call("get_json_string", fid, varchar_type(), {document, text(path)});
    }

    Expr* compare(TExprOpcode::type opcode, Expr* left, Expr* right) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::BINARY_PRED);
        node.__set_opcode(opcode);
        node.__set_num_children(2);
        node.__set_is_nullable(true);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        node.__set_child_type_desc(varchar_type().to_thrift());
        Expr* predicate = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(node));
        predicate->add_child(left);
        predicate->add_child(right);
        return predicate;
    }

    RuntimeState _state;
    ObjectPool _pool;
    std::vector<ExprContext*> _contexts;
};

// Args: {number of conjuncts, every n-th row holds "ATM", 1 = with the cache, 1 = grouped}
void BM_conjuncts(benchmark::State& state) {
    const ColumnPtr docs = documents(state.range(1));
    Chunk chunk;
    chunk.append_column(docs, Conjuncts::kSlot);
    Conjuncts conjuncts(state.range(0), state.range(3) != 0);
    const bool cache = state.range(2) != 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(cache ? conjuncts.with_cache(&chunk) : conjuncts.without_cache(&chunk));
    }
    finish(state, docs);
}

BENCHMARK(BM_vpack_reference)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_extract_one_path)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_extract_three_paths)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_conjuncts)
        ->Unit(benchmark::kMillisecond)
        ->Args({1, 1, 1, 1})
        ->Args({3, 1, 1, 1})
        ->Args({3, 1, 0, 1})
        ->Args({3, 1, 0, 0})
        ->Args({1, 33, 1, 1})
        ->Args({3, 33, 1, 1})
        ->Args({3, 33, 0, 1})
        ->Args({3, 33, 0, 0});

} // namespace

} // namespace starrocks

BENCHMARK_MAIN();
