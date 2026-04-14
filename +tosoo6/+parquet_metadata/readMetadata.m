function metadata = readMetadata(filename)
%READMETADATA Extract user-defined key-value metadata from a Parquet file
%   S = parquet_metadata.readMetadata(FILENAME) returns a struct S with user metadata.
%
%   Behavior
%   - Returns user key_value_metadata as a MATLAB struct with one field per
%     metadata key (keys are sanitized to valid MATLAB field names).
%   - JSON decoding: values that look like JSON objects/arrays ("{...}", "[...]"),
%     booleans (true/false), null, or quoted strings are automatically decoded via
%     jsondecode. Numeric-looking bare strings (e.g. '12345', '3.14', '1e3') are
%     preserved as strings to avoid accidental type changes.
%   - All other footer fields (schema, row groups, etc.) are not exposed.
%
%   Notes on field names
%   - Keys are converted to valid MATLAB field names (non-alphanumerics replaced
%     with underscore, leading digits prefixed) to allow convenient S.key access.
%
%   Example:
%       S = parquet_metadata.readMetadata('data.parquet');
%       fieldnames(S)
%       S.my_key
%       S.configuration.StimulationPattern  % Access parsed JSON

    % Validate input
    if ~isfile(filename)
        error('parquet_metadata:readMetadata:FileNotFound', 'File not found: %s', filename);
    end
    
    % Open file for binary reading
    fid = fopen(filename, 'rb');
    if fid == -1
        error('parquet_metadata:readMetadata:CannotOpenFile', 'Cannot open file: %s', filename);
    end
    
    % Ensure file is closed on exit
    cleanup = onCleanup(@() fclose(fid)); %#ok<NASGU>
    
    try
        % Read and validate footer
        footer_info = readFooter(fid);
        
        % Read FileMetaData
        file_metadata = readFileMetadata(fid, footer_info);
        
        % Extract key-value metadata
        metadata = extractKeyValueMetadata(file_metadata);
        
    catch ME
        % Re-throw with more context
        error('parquet_metadata:readMetadata:ParseError', ...
            'Failed to parse Parquet metadata: %s', ME.message);
    end
end

function footer_info = readFooter(fid)
    % Read and validate Parquet file footer
    
    % Get file size
    fseek(fid, 0, 'eof');
    file_size = ftell(fid);
    
    if file_size < 12  % Minimum: 4(PAR1) + 4(length) + 4(PAR1)
        error('File too small to be a valid Parquet file');
    end
    
    % Read trailing magic number
    fseek(fid, -4, 'eof');
    trailing_magic = fread(fid, 4, 'uint8');
    
    if ~isequal(trailing_magic', [80, 65, 82, 49])  % 'PAR1'
        error('Invalid Parquet file: missing trailing magic number');
    end
    
    % Read footer length
    fseek(fid, -8, 'eof');
    footer_length = fread(fid, 1, 'uint32', 0, 'ieee-le');
    
    if footer_length <= 0 || footer_length > file_size - 12
        error('Invalid footer length: %d', footer_length);
    end
    
    % Position at start of FileMetaData
    metadata_start = file_size - 8 - footer_length;
    fseek(fid, metadata_start, 'bof');
    
    % Validate leading magic number
    fseek(fid, 0, 'bof');
    leading_magic = fread(fid, 4, 'uint8');
    
    if ~isequal(leading_magic', [80, 65, 82, 49])  % 'PAR1'
        error('Invalid Parquet file: missing leading magic number');
    end
    
    footer_info.metadata_start = metadata_start;
    footer_info.metadata_length = footer_length;
end

function file_metadata = readFileMetadata(fid, footer_info)
    % Read FileMetaData using Thrift Compact Protocol
    
    fseek(fid, footer_info.metadata_start, 'bof');
    
    % Read all metadata bytes
    metadata_bytes = fread(fid, footer_info.metadata_length, 'uint8');
    
    % Parse Thrift Compact Protocol
    parser = tosoo6.parquet_metadata.ThriftCompactParser(metadata_bytes);
    file_metadata = parser.readStruct();
end

function metadata = extractKeyValueMetadata(file_metadata)
    % Extract key-value pairs from FileMetaData structure and return as struct
    
    metadata = struct();
    
    % Field 5 in FileMetaData is key_value_metadata (optional list<KeyValue>)
    if isfield(file_metadata, 'field_5') && ~isempty(file_metadata.field_5)
        kv_list = file_metadata.field_5;
        
        for i = 1:length(kv_list)
            kv_pair = kv_list{i};
            
            % KeyValue struct: field_1 = key (required string), field_2 = value (optional string)
            if isfield(kv_pair, 'field_1')
                key = kv_pair.field_1;
                value = '';
                
                if isfield(kv_pair, 'field_2')
                    value = kv_pair.field_2;
                end
                
                % Try to parse as JSON
                parsed_value = parseJsonValue(value);
                
                % Create valid field name for struct
                field_name = makeValidFieldName(key);
                metadata.(field_name) = parsed_value;
            end
        end
    end
end

function parsed_value = parseJsonValue(value)
    % Try to parse value as JSON, return original if not JSON
    
    if isempty(value)
        parsed_value = value;
        return;
    end
    
    % Heuristics: attempt to decode objects, arrays, booleans, null, quoted strings
    trimmed = strtrim(value);
    looks_json = false;
    if ~isempty(trimmed)
        if startsWith(trimmed, '{') || startsWith(trimmed, '[') || startsWith(trimmed, '"')
            looks_json = true;
        elseif any(strcmpi(trimmed, {"true","false","null"}))
            looks_json = true;
        end
    end
    if looks_json
        try
            parsed_value = jsondecode(trimmed);
            return;
        catch
            % fall through to return original string
        end
    end
    parsed_value = value;
end

function field_name = makeValidFieldName(key)
    % Convert key to valid MATLAB field name
    
    % Handle common special cases first
    if isempty(key)
        field_name = 'unnamed';
        return;
    end
    
    % Replace invalid characters with underscores
    field_name = regexprep(key, '[^a-zA-Z0-9_]', '_');
    
    % Handle consecutive underscores (replace multiple with single)
    field_name = regexprep(field_name, '_+', '_');
    
    % Remove leading/trailing underscores
    field_name = regexprep(field_name, '^_+|_+$', '');
    
    % Ensure it starts with a letter (prepend 'x' if starts with number or underscore)
    if ~isempty(field_name) && ~isletter(field_name(1))
        field_name = ['x' field_name];
    end
    
    % Ensure it's not empty after cleanup
    if isempty(field_name)
        field_name = 'unnamed';
    end
end

