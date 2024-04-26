find_program(CLANG_EXECUTBALE NAMES "clang" "clang.exe" PATHS "C:/Program Files/LLVM/bin")
if (NOT CLANG_EXECUTBALE)
	message(FATAL_ERROR "Unable to find clang")
endif()

get_filename_component(LLVM_BIN_DIR "${CLANG_EXECUTBALE}" DIRECTORY)

set(CMAKE_C_COMPILER "${LLVM_BIN_DIR}/clang-cl.exe" CACHE STRING "")
set(CMAKE_CXX_COMPILER "${LLVM_BIN_DIR}/clang-cl.exe" CACHE STRING "")
set(CMAKE_AR "${LLVM_BIN_DIR}/llvm-lib.exe" CACHE STRING "")
set(CMAKE_LINKER "${LLVM_BIN_DIR}/lld-link.exe" CACHE STRING "")
set(CMAKE_RC_COMPILER "${LLVM_BIN_DIR}/llvm-rc.exe" CACHE STRING "")
