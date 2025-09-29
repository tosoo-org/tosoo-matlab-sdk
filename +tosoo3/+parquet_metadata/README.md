# Parquet Metadata Reader for MATLAB

from: https://github.com/simonacca/parquetmatlab

A pure MATLAB implementation for reading user-defined key-value metadata from Apache Parquet files.

## Problem

MATLAB's built-in `parquetread` and `parquetinfo` functions don't provide access to user-defined key-value metadata stored in Parquet files. This metadata is often crucial for understanding data provenance, processing parameters, and other important information.

## Solution

This package implements a minimal Thrift Compact Protocol parser to directly read the metadata from Parquet files, without requiring external dependencies like Python or Java.


## Usage

## Write metadata

```matlab
% Start with any Parquet file. For example, create a tiny one:
T = table((1:3)');
parquetwrite('data.parquet', T);

% Option A: provide metadata as a struct
kv = struct();
kv.created_by = 'matlab-writer';        % written as string
kv.params = struct('alpha', 0.1);       % non-strings are JSON-encoded
parquet_metadata.writeMetadata('data.parquet', kv);

% Option B: as containers.Map (keys written exactly as provided)
m = containers.Map({'created_by','version'},{'matlab','1.0'});
parquet_metadata.writeMetadata('data.parquet', m);

% Option C: as an N×2 cell array {key, value}
pairs = {
  'description', 'demo file';
  'thresholds', [1,2,3]    % will be JSON-encoded
};
parquet_metadata.writeMetadata('data.parquet', pairs);

% Read back
md = parquet_metadata.readMetadata('data.parquet');
disp(md);
```

### Read metadata

```matlab
% Read metadata from the same file written above
md = parquet_metadata.readMetadata('data.parquet');

% List available keys (struct fields)
disp('Available metadata keys:');
disp(fieldnames(md));

% Access the values from the last write example
if isfield(md, 'description')
    fprintf('Description: %s\n', md.description);
end
if isfield(md, 'thresholds')
    disp('Thresholds:');
    disp(md.thresholds);  % auto-decoded from JSON to numeric array
end
```



Notes on writing:
- Overwrite behavior: the existing user key_value_metadata is replaced by the
  provided pairs.
- Value encoding: non-char values are JSON-encoded via `jsonencode`. Strings
  are written as-is. Pass a string/char to force a literal string.


## Testing

We validate correctness by reading and writing files between matlab and python (which has a well-known implementation in pyarrow).

- write in python, read in matlab
- write in matlab, read in python
