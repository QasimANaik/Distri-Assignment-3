#!/bin/bash

INPUT_FILE=${1:-input.txt}
OUTPUT_FILE=${2:-output.txt}

# Step 1: Mapper
# Step 2: Sort (like Hadoop shuffle)
# Step 3: Combiner
# Step 4: Sort again
# Step 5: Reducer
# Step 6: Sort final output

python3 mapper.py < "$INPUT_FILE" | \
    sort | \
    python3 combiner.py | \
    sort | \
    python3 reducer.py > "$OUTPUT_FILE"

echo "MapReduce pipeline completed. Output saved to $OUTPUT_FILE"