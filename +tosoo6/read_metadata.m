function metadata = read_metadata(input_file)
%READ_METADATA Read metadata from Tosoo6 parquet file.
%   METADATA = READ_METADATA(INPUT_FILE) reads metadata from a Tosoo6
%   parquet file and returns a struct containing the metadata.
%
%   Input:
%       input_file - Path to the parquet file (string or char array)
%
%   Output:
%       metadata - Struct containing Tosoo6 metadata fields:
%           - device_mac_address
%           - firmware_version
%           - recording_start_datetime
%           - file_format_version
%           - configuration (nested struct)
%           - eeg_sampling_frequency_hz
%           - actigraphy_sampling_frequency_hz
%           - eeg_unit_of_measurement
%           - actigraphy_unit_of_measurement
%
%   Example:
%       metadata = tosoo6.read_metadata('recording.tosoo6.parquet');

    % Validate input
    if ~isfile(input_file)
        error('tosoo6:read_metadata:FileNotFound', 'File not found: %s', input_file);
    end

    % Use the parquet_metadata package to read raw metadata
    raw_metadata = tosoo6.parquet_metadata.readMetadata(input_file);

    % Check if 'metadata' field exists (the actual Tosoo6 metadata is stored here)
    if isfield(raw_metadata, 'metadata')
        % The metadata field contains the actual Tosoo6 metadata as a struct
        metadata = raw_metadata.metadata;
    else
        % If no metadata field, return the raw metadata
        metadata = raw_metadata;
    end

end