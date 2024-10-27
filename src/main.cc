#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <iostream>
#include <memory>
#include <string>

int main() {
  const std::string file{"../ipc/sample.arrow"};

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

  for (std::size_t i = 0; i < std::size(x); ++i) {
    std::cout << i << ": " << x[i] << " " << y[i];
    for (std::size_t j = 0; j < std::size(z); ++j) {
      std::cout << " " << z[j][i];
    }
    std::cout << "\n";
  }

  return 0;
}
