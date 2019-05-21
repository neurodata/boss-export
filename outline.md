# Outline

## Transfer a dataset out of BOSS

### Requirements

1. Collection name
1. Experiment name
1. Channel name

### Steps

1. Create instance w/in AWS BOSS environment
1. Connect to the mysql table
1. Extract Collection ID, Experiment ID, Channel ID from mysql query
1. Extract metadata (from coordinate_frame and experiment (through sql query)
   1. Extents
   1. Datatype (`uint8`/`uint16`/`uint64`)
   1. Voxel sizes
   1. Voxel unit
   1. Number of hierarchy levels (res)
1. Determine the list of objects to transfer (for each res)
1. Dump the list of objects into SQS
1. Create a neuroglancer precomputed path/volume
1. Iterate over SQS items transferring them out and deleting each message as it completes
