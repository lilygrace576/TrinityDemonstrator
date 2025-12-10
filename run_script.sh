#!/bin/bash
# run_script.sh

DATE=$1
FILE=$2
echo "Running for $DATE and File $FILE"
apptainer exec --bind /storage/osg-otte1/shared/TrinityDemonstrator:/mnt /storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/containers/rootandexact.sif /mnt/DataAnalysis/MergedData/scripts/MergeData/FileMerge $DATE /mnt/ $FILE
# sleep 10
apptainer exec --bind /storage/osg-otte1/shared/TrinityDemonstrator:/mnt /storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/containers/rootandexact.sif /mnt/DataAnalysis/MergedData/scripts/IncludeCalibrationData/AddCalibData $DATE /mnt/ Merged_$FILE
chmod 774 Output/$DATE
chmod 664 Output/$DATE/*
