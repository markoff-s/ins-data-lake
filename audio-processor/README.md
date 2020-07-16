ins-data-lake-audio processor

1. launch stack from ins-data-lake/audio-processor/cloud-formation/audio-recog.yml

2. create folder in audio processor bronse bucket for incoming files.
   2.1. subject of external integration; 
   2.2. any name; 
   2.3. incoming meta data files must refer the same folder
   2.4. this folder will be used for incoming audio files storage
   2.5. audio file processing is initiated by any PUT(*) event for incoming meta data (json) files
   2.6. meta data file example:
   {
      "CustomerId": 3,
      "ClaimId": 1022,
      "RecordPath": "audio/5b78d6b9-6f4b-482b-8985-330ac20d4288_20200618T18_36_UTC.wav"
   }

3. audio files and corresponding meta data files can be found by path \ins-data-lake\audio-processor\incoming-files
   3.1. upload audio files to target folder (audio) in bronse bucket first
   3.2. upload meta data (json) files to the root of bronse bucket after step 3.1. is completed

