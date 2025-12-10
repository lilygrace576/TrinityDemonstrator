#!/bin/bash
# run_condor.sh

source /opt/root/bin/thisroot.sh

# Setup ExACT
export EXACT_DIR=/exact
export LD_LIBRARY_PATH=/exact/dict:/usr/lib/oracle/21/client64/lib:/usr/local/lib:$LD_LIBRARY_PATH


DATE=$1
FILENAME=$2
echo "Running job for $DATE with file $FILENAME "
echo "Directories"
ls -lh

# Create necessary directories if they don't exist
mkdir DataAnalysis
chmod 777 DataAnalysis
mkdir Data
mkdir MiscData
cd DataAnalysis
mkdir exact
mkdir MergedData
mkdir AncillaryData
cd MergedData
mkdir Output
cd Output
mkdir $DATE
cd
cd DataAnalysis/exact
mkdir data
cd
cd Data
mkdir $DATE
cd $DATE
mkdir RawDataMerged
cd
mv $FILENAME Data/$DATE/RawDataMerged/.
cd DataAnalysis/AncillaryData
mkdir Data2
mkdir Data1
cd
mv statemessages${DATE}.csv DataAnalysis/AncillaryData/Data2/.
mv celestialPositions${DATE}.csv DataAnalysis/AncillaryData/Data1/.
mv UCTempatureCorrections.csv DataAnalysis/exact/data/.
mv SiPMTempatureCorrections.csv DataAnalysis/exact/data/.
cd 
cd MiscData
mkdir WeatherData
cd WeatherData
mkdir weather
cd
mv weather_${DATE} MiscData/WeatherData/weather/.


echo "Running FileMerge for $DATE and File $FILENAME"
# ldd ./FileMerge
./FileMerge $DATE /srv/ $FILENAME
# sleep 10
echo "Running Calibration for $DATE and File $FILENAME"
./AddCalibData $DATE /srv/ Merged_$FILENAME
ls -lh

cd 
cd DataAnalysis/MergedData/Output/$DATE/
chmod 774 Merged_${FILENAME}
pwd
ls -lh

cd
pwd
ls -lh