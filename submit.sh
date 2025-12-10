#!/bin/bash

INPUT=$1
# Check for date argument
if [ -z "$INPUT" ]; then
  echo "Usage: $0 <YYYYMMDD | date_file>"
  exit 1
fi

SUBMIT_TEMPLATE="condense_SM.submit"
BASE_DIR="/storage/osg-otte1/shared/TrinityDemonstrator/Data"
# BASE_DIR="/storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/MergedData/Output"

# Determine if input is a file or a single date
if [ -f "$INPUT" ]; then
  echo "Reading dates from file: $INPUT"
  DATES=$(grep -v '^#' "$INPUT" | grep -E '^[0-9]{8}$')
else
  # Validate date format
  if [[ ! "$INPUT" =~ ^[0-9]{8}$ ]]; then
    echo "Error: '$INPUT' is not a valid YYYYMMDD or a file."
    exit 1
  fi
  DATES="$INPUT"
fi

echo "Using submit template: $SUBMIT_TEMPLATE"
echo "---------------------------------------------"

for DATE in $DATES; do
  TARGET_DIR="${BASE_DIR}/${DATE}/RawDataMerged/"

  if [ ! -d "$TARGET_DIR" ]; then
    echo "Directory not found: $TARGET_DIR — skipping."
    continue
  fi

  echo "Processing date: $DATE"
  
  OUTLIST="/storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/data_lists/file_list_${DATE}.txt"

  rm -f "$OUTLIST"
  for f in "$TARGET_DIR"/*.root; do
      echo "${f##*/}" >> "$OUTLIST"
  done

  echo "Created $OUTLIST with $(wc -l < "$OUTLIST") files"
  #echo "Submitting job for: $BASENAME"
  condor_submit Date="$DATE" "$SUBMIT_TEMPLATE"

  #echo "Submitted all jobs for date $DATE"
  #echo "---------------------------------------------"
done

echo "All submissions complete."




mkdir -p /storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/MergedData/Output/$DATE
echo "Watcher started..."
apptainer exec --bind /storage/osg-otte1/shared/TrinityDemonstrator:/mnt \
  /storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/containers/python3_10.sif \
  python3 /storage/osg-otte1/shared/TrinityDemonstrator/DataAnalysis/MergedData/watcher.py \
  -i /mnt/DataAnalysis/MergedData/ \
  -o /mnt/DataAnalysis/MergedData/Output/ \
  -l /mnt/DataAnalysis/MergedData/.logs/watcher.log \
  -t 300
echo "Watcher finished."


# DATE=$1

# # Loop through files matching the date
# for FILE in "$TARGET_DIR"*; do
#   [ -e "$FILE" ] || continue  # skip if no match

#   BASENAME=$(basename "$FILE")
#   echo "Submitting job for file: $FILE (basename: $BASENAME)"
#   condor_submit Date=$DATE Filename=$BASENAME "$SUBMIT_TEMPLATE"
# done
# echo "Submitted all jobs for date $DATE"