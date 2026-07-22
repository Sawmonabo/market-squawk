# ONNX Runtime optional native component notice

Market Squawk can optionally load an operator-supplied ONNX Runtime 1.24.4 CPU shared library. The
library is not distributed by Market Squawk, is not required by the product, and is never fetched by
the build or application. The self-contained `tract-onnx` backend remains the required ONNX runtime.

| Inventory field | Value |
| --- | --- |
| Component | Microsoft ONNX Runtime |
| Admitted version | 1.24.4 |
| Supplier | Microsoft Corporation and contributors |
| License | MIT |
| Relationship | Optional operator-supplied native runtime; dynamically loaded after local admission |
| Runtime artifacts | `libonnxruntime.dylib`, `libonnxruntime.so`, or `onnxruntime.dll`, as applicable |
| Release | <https://github.com/microsoft/onnxruntime/releases/tag/v1.24.4> |
| License source | <https://github.com/microsoft/onnxruntime/blob/v1.24.4/LICENSE> |
| Upstream source | <https://github.com/microsoft/onnxruntime/tree/v1.24.4> |

ONNX Runtime is licensed under the MIT License. An operator who provisions the optional shared
library must retain the upstream copyright and permission notice with that library and include the
exact library hash, platform, version, and this inventory record in release evidence. Task 20's
release SBOM must inventory the native library when the optional backend is enabled; omission of the
optional library means the tract backend is used and does not block the local release.

Market Squawk's tracked verifier policy binds the digest of this notice. Replacing the library,
version, platform, policy, or notice requires new admission evidence before the optional backend can
be constructed.
