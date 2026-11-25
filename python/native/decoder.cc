#include "spooltail.pb.h"
#include <pybind11/pybind11.h>
#include <string_view>

namespace py = pybind11;
namespace schema = spooltail::v1;

static py::dict object(const schema::ReplayObject& fields);

static py::object value(const schema::ReplayValue& v) {
    switch (v.kind_case()) {
        case schema::ReplayValue::kNullValue: return py::none();
        case schema::ReplayValue::kBoolean: return py::bool_(v.boolean());
        case schema::ReplayValue::kInteger: return py::int_(v.integer());
        case schema::ReplayValue::kPositive: return py::int_(v.positive());
        case schema::ReplayValue::kReal: return py::float_(v.real());
        case schema::ReplayValue::kText: return py::str(v.text());
        case schema::ReplayValue::kArray: {
            py::list items;
            for (const auto& item : v.array().items()) items.append(value(item));
            return items;
        }
        case schema::ReplayValue::kObject: return object(v.object());
        default: throw py::value_error("missing replay value");
    }
}

static py::dict object(const schema::ReplayObject& fields) {
    py::dict result;
    for (const auto& field : fields.fields())
        result[py::str(field.key())] = value(field.value());
    return result;
}

PYBIND11_MODULE(spooltail_replay_native, m) {
    m.def("decode_object", [](py::bytes data) {
        std::string_view bytes = data;
        schema::ReplayObject message;
        if (bytes.size() > 2 * 1024 * 1024 ||
            !message.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())))
            throw py::value_error("invalid replay object");
        return object(message);
    });
}
