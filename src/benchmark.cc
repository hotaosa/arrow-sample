#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>
#include <string>

static void BM_Load(benchmark::State &state) {
  const std::string file{"../ipc/sample.arrow"};

  for (auto _ : state) {
    std::shared_ptr<arrow::io::ReadableFile> input;
    arrow::Status status = arrow::io::ReadableFile::Open(file).Value(&input);
    assert(status.ok());

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
    status = arrow::ipc::RecordBatchFileReader::Open(input).Value(&reader);
    assert(status.ok());

    std::vector<double> x, y;
    std::vector<std::vector<double>> z(24);
    x.reserve(1500);
    y.reserve(1500);
    for (auto &zi : z) zi.reserve(1500);

    std::shared_ptr<arrow::RecordBatch> batch;
    status = reader->ReadRecordBatch(0).Value(&batch);
    assert(status.ok());
    auto xp = std::static_pointer_cast<arrow::DoubleArray>(batch->column(0));
    auto yp = std::static_pointer_cast<arrow::DoubleArray>(batch->column(1));
    std::vector<std::shared_ptr<arrow::DoubleArray>> zp;
    for (int i = 2; i < 26; ++i) {
      zp.emplace_back(
          std::static_pointer_cast<arrow::DoubleArray>(batch->column(i)));
    }

    for (int i = 0; i < batch->num_rows(); ++i) {
      x.emplace_back(xp->Value(i));
      y.emplace_back(yp->Value(i));
      for (std::size_t id = 0; id < std::size(z); ++id) {
        z[id].emplace_back(zp[id]->Value(i));
      }
    }
  }
}
BENCHMARK(BM_Load)->Unit(benchmark::kMicrosecond)->MinWarmUpTime(1);
BENCHMARK_MAIN();
