## Detchannelmaps and DUNE DAQ Library Installation
Access to the DUNE DAQ library and detchannelmaps is required to run mapping methods with the Graph Data Extractor. Ronald Rechenmacher (https://github.com/ron003) provided a script that installs DAQ as well as makes the necessary APIs available. The source for these scripts is available [here](https://github.com/ron003/script/blob/main/DUNE-DAQ_IGX_detchannelmaps_test_root.sh).
### Installation
1. cd to wibviz_graph/scripts
2. Install dependencies with `./bootstrap_DUNEDAQ.sh`
3. Install the DUNE DAQ Library with `./DUNE-DAQ_IGX_detchannelmaps_test_root.sh`
4. You should now see a `test_root` folder. cd to `test_root`.
5. Start the venv with `source env.sh` or `. ./env.sh`

You should see `(dbt)` as your venv. Run `./DUNE-DAQ_run_ICEBERGChannelMap_to_wire.sh` to ensure your environment installed correctly. 

### Troubleshooting

If you run into issues with finding the compiled .so files or other libraries within test_root, try running `export LD_LIBRARY_PATH`.

This version of DAQ only has known compatibility with Ubuntu 22. There have been issues with Ubuntu 20.