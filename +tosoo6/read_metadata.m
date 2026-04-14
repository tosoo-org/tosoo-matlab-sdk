function metadata = read_metadata(input_file)
%READ_METADATA Read stream_header metadata from Tosoo6 parquet file.
%   METADATA = READ_METADATA(INPUT_FILE) reads the stream_header metadata
%   from a Tosoo6 parquet file and returns a struct containing it.
%
%   Input:
%       input_file - Path to the parquet file (string or char array)
%
%   Output:
%       metadata - Struct containing the stream_header fields
%
%   Example:
%       metadata = tosoo6.read_metadata('recording.tosoo6.parquet');

    % Validate input
    if ~isfile(input_file)
        error('tosoo6:read_metadata:FileNotFound', 'File not found: %s', input_file);
    end

    % Use the parquet_metadata package to read raw metadata
    raw_metadata = tosoo6.parquet_metadata.readMetadata(input_file);

    % Extract stream_header from metadata
    if isfield(raw_metadata, 'metadata') && isfield(raw_metadata.metadata, 'stream_header')
        metadata = raw_metadata.metadata.stream_header;
    elseif isfield(raw_metadata, 'stream_header')
        metadata = raw_metadata.stream_header;
    else
        error('tosoo6:read_metadata:NoStreamHeader', 'stream_header not found in metadata');
    end

end