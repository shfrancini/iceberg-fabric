#!/bin/bash
set -e
#set -uo pipefail

usage() {
  echo
  echo "Usage: $0 [--root-dir <dir>] [--ssh]"
  echo
  echo "Options:"
  echo "  --root-dir <dir>  Root working directory (default: test_root)"
  echo "  --ssh             Use SSH clone URLs (git@github.com:)"
  echo "  -h, --help, -?    Show this help message"
  echo
}

set -u

coredaq_ver=coredaq-v4.5.8
TEST_ROOT=test_root
USE_SSH=0
GITHUB_BASE="https://github.com/"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root-dir)
      shift
      test $# -eq 0 && { echo "Error: --root-dir requires a directory argument" >&2; usage; exit 1; }
      TEST_ROOT="$1"
      ;;
    --ssh)         USE_SSH=1;;
    -h|--help|-?)  usage; exit 0;;
    *)             echo "Error: Unknown argument: $1" >&2; usage; exit 1;;
  esac
  shift
done

if [[ "$USE_SSH" -eq 1 ]]; then
  GITHUB_BASE="git@github.com:"
fi

clone_if_missing() {
  local repo_path="$1"
  shift
  local repo_name="${repo_path##*/}"
  local target_dir="${repo_name%.git}"

  if [[ -d "$target_dir" ]]; then
    echo "Warning: Directory '$target_dir' already exists; skipping clone of ${GITHUB_BASE}${repo_path}"
    return 0
  else
    echo "Cloning ${GITHUB_BASE}${repo_path} into '$target_dir'..."
  fi

  git clone "${GITHUB_BASE}${repo_path}" "$@"
}

if [[ -d "$TEST_ROOT" ]]; then
  echo "Using existing root directory: $TEST_ROOT"
else
  echo "Creating root directory: $TEST_ROOT"
  mkdir -p "$TEST_ROOT"
fi

cd "$TEST_ROOT"

clone_if_missing DUNE-DAQ/daq-buildtools -b $coredaq_ver # normally at /cvmfs/...
clone_if_missing DUNE-DAQ/daq-release    -b $coredaq_ver

echo
echo The 4 externals repos: Catch2, cetmodules, cetlib-except, cetlib
echo

test -d externals || mkdir externals; install_dir=$PWD/install; test -d $install_dir || mkdir -p $install_dir
cd externals

clone_if_missing FNALssi/cetmodules -b 3.18.00
cd cetmodules;test -d build || mkdir $_; cd $_
cmake -DBUILD_DOCS=0 -DCMAKE_INSTALL_PREFIX=$PWD ..;make -j$(nproc) && make install
cetmodules=$PWD;cd ../..

clone_if_missing catchorg/Catch2 -b v2.13.10  #v3.12.0
cd Catch2; test -d build || mkdir $_;cd $_
cmake -DCMAKE_INSTALL_PREFIX=$install_dir .. && make -j$(nproc) && make install
cd ../..

clone_if_missing art-framework-suite/cetlib-except -b v1_07_04
cd cetlib-except
grep -q '(void)' cetlib_except/test/exception_test.cc\
    || sed -i 's/std::equal(/(void)std::equal(/' $_ # PATCH
test -d build || mkdir $_; cd $_
CMAKE_PREFIX_PATH=$cetmodules:$install_dir/lib/cmake/Catch2 \
cmake -DCMAKE_INSTALL_PREFIX=$install_dir .. && make -j$(nproc)
test -d CMakeFiles/Export/lib/cetlib_except || mkdir -p $_  # deal with uber new cmake
test -h CMakeFiles/Export/lib/cetlib_except/cmake || ln -s ../../cba13e0e966cbf40c2dc2e28d3a59f59 $_
make install
cd ../..

clone_if_missing art-framework-suite/cetlib -b v3_18_01; cd cetlib
# the presence of the build dir is the only way to know if the patch has been applied, and it needs to be applied before the build dir is made
test -d build || patch -p1 <../../daq-release/spack-repos/externals/packages/cetlib/cetlib_lite.patch
test -d build || mkdir $_; cd $_
CMAKE_PREFIX_PATH=$cetmodules:$install_dir/lib/cmake/Catch2:$install_dir/lib/cetlib_except/cmake \
cmake -DBUILD_TESTING=FALSE -DCMAKE_INSTALL_PREFIX=$install_dir ..; make -j$(nproc)
test -d CMakeFiles/Export/lib/cetlib || mkdir -p $_  # deal with uber new cmake
test -h CMakeFiles/Export/lib/cetlib/cmake || ln -s ../../d6cf62c7c6d24d3aa088679a86b1376f $_
make install
cd ../..

cd ..
echo
echo DONE with 4 "externals" - pwd=`pwd`
echo

echo ". daq-buildtools/env.sh
#dbt-workarea-env
export DBT_AREA_ROOT=$PWD

#externals:
echo \":\${LD_LIBRARY_PATH-}:\" | grep -q \":$PWD/externals/lib\" || LD_LIBRARY_PATH=$PWD/externals/lib\${LD_LIBRARY_PATH+:\$LD_LIBRARY_PATH}
export LD_LIBRARY_PATH

for dd in \`find install -name bin\`;do
  echo \":\$PATH:\" | grep -q \":$PWD/\$dd:\" || { echo Adding \$dd to PATH; PATH=$PWD/\$dd:\$PATH;}
done
for dd in \`find install -type d -name lib\*\`;do
  echo \":\$LD_LIBRARY_PATH:\" | grep -q \":$PWD/\$dd:\" || LD_LIBRARY_PATH=$PWD/\$dd:\$LD_LIBRARY_PATH
done
for dd in \`find install -name python\`;do
  echo \":\$PYTHONPATH:\" | grep -q \":$PWD/\$dd:\" || { echo Adding \$dd to PYTHONPATH; export PYTHONPATH=$PWD/\$dd:\$PYTHONPATH;}
done
for dd in \`find install -name share\`;do
   export \$(basename \`dirname \$dd\` | tr 'a-z-' 'A-Z_')_SHARE=$PWD/\$dd
done
test -d .venv || python3 -m venv --prompt dbt .venv
unset _OLD_VIRTUAL_PATH # a tricky detail - partial \"deactivate\"
. .venv/bin/activate"'
type trace_functions.sh >/dev/null 2>&1 && {
    . trace_functions.sh; export TRACE_MSGMAX=0
}

dbt-build() {
    local DD= CLEAN=0 VERBOSE=0 OPTIND=0
    while getopts ":dvc" opt; do
        case "$opt" in
          d) DD="-DDBT_DEBUG=1 -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS_DEBUG=\"-O0 -g\"";;
          c) CLEAN=1 ;;
          v) VERBOSE=1 ;;
          \?) echo "Invalid option: -$OPTARG"; return 1 ;;
        esac
    done
    shift $((OPTIND-1))
    pushd $DBT_AREA_ROOT/build >/dev/null || return
    ((CLEAN)) && rm -rf ../build/*
    logfile=build_attempt_`date|sed "s/[ :][ :]*/_/g"`.log

    CMAKE_PREFIX_PATH=\
`echo $DBT_AREA_ROOT/.venv/lib/python3.*/site-packages/pybind11`:\
$DBT_AREA_ROOT/install/lib/cetlib/cmake:\
$DBT_AREA_ROOT/install/lib/cetlib_except/cmake

    eval CMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH" \
cmake -DCMAKE_MODULE_PATH=$DBT_ROOT/cmake -DCMAKE_INSTALL_PREFIX=$DBT_AREA_ROOT/install $DD ../sourcecode 2>&1 | tee ../log/$logfile
    make_flags="-j$(nproc)"
    ((VERBOSE)) && make_flags+=" VERBOSE=1"
    ( make $make_flags && make install ) 2>&1 | tee -a ../log/$logfile

    echo log file at $DBT_AREA_ROOT/log/$logfile
    popd >/dev/null
}
' >env.sh

touch dbt-workarea-constants.sh

echo
echo 'Install some sourcecode in the sourcecode subdirectory...'
echo

test -d sourcecode || mkdir $_; cd $_
cp ../daq-buildtools/configs/CMakeLists.txt .
cp ../daq-release/configs/fddaq/fddaq-v4.4.8/dbt-build-order.cmake .
clone_if_missing DUNE-DAQ/daq-cmake        -b $coredaq_ver
clone_if_missing DUNE-DAQ/ers              -b coredaq-v5.5.0 #$coredaq_ver
clone_if_missing DUNE-DAQ/logging          -b coredaq-v5.5.0 #ron/address_warnings #$coredaq_ver
clone_if_missing DUNE-DAQ/detdataformats   -b coredaq-v5.4.3
clone_if_missing DUNE-DAQ/fddetdataformats
sed -i 's/bindings(\*.cpp *)/bindings(*.cpp LINK_LIBRARIES ${PROJECT_NAME})/' fddetdataformats/CMakeLists.txt
#clone_if_missing DUNE-DAQ/detchannelmaps -b $coredaq_ver
clone_if_missing ron003/detchannelmaps -b ron/run_channel_map_api

clone_if_missing art-daq/trace -b develop #v3_17_14  # v3_20_00
grep -q trace dbt-build-order.cmake || sed -i '/daq-cmake/a\
                "trace"' dbt-build-order.cmake
# icebergchanneltowire is special with respect to it's github codespace
# it might be checked out (by the codespace) in ".."
test \! -h icebergchanneltowire -a -d ../icebergchanneltowire && ln -s $_ .
clone_if_missing ron003/icebergchanneltowire
test -h .vscode || ln -s icebergchanneltowire/.vscode .
grep -q icebergchanneltowire dbt-build-order.cmake || sed -i '/fddetdataformats/a\
                "icebergchanneltowire"' dbt-build-order.cmake
cd ..


echo 'Executinging . env.sh'
. env.sh   # needs to see ./dbt-workarea-constants.sh; get my dbt-build

pip install pybind11

cd $DBT_AREA_ROOT
test -d build || mkdir $_
test -d log || mkdir $_
dbt-build
echo "Now: cd $TEST_ROOT; . ./env.sh"
