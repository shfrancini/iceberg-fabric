sudo apt-get update
sudo apt install -y libabsl-dev libgoogle-glog-dev libgtest-dev libtbb-dev libpng-dev protobuf-compiler sqlite3
sudo apt install -y build-essential cmake git patch python3-pip python3-venv python3-dev libssl-dev libboost-all-dev
sudo apt-get install libsqlite3-dev doxygen texlive-latex-recommended -y

chmod +x ./DUNE-DAQ_IGX_detchannelmaps_test_root.sh 

./DUNE-DAQ_IGX_detchannelmaps_test_root.sh # Build the DAQ libraries
