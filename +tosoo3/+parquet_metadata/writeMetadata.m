function writeMetadata(filePath, kv)
%writeMetadata Overwrite user key-value metadata in a Parquet file (in place).
%   parquet_metadata.writeMetadata(filePath, kv)
%   - filePath: path to Parquet file to modify in place
%   - kv: struct, containers.Map, or Nx2 cell array {key, value}
%
%   Behavior
%   - Overwrites user key_value_metadata (field id 5) so that it contains
%     exactly the pairs provided in kv. All other footer fields are preserved.
%   - JSON encoding: non-char values are JSON-encoded via jsonencode before
%     writing so MATLAB structs/arrays are stored and can be parsed on read.
%     Strings are written as-is (UTF-8). To force a value to be treated as
%     a string, pass a char/string; to store numbers/booleans/arrays/objects,
%     pass MATLAB types and they will be JSON-encoded.
%   - Keys are written exactly as provided when using containers.Map or
%     {key,value} cells. When using a struct, MATLAB fieldname constraints
%     apply to the provided keys.

    arguments
        filePath (1,:) char
        kv
    end

    if ~isfile(filePath)
        error('parquet_metadata:writeMetadata:FileNotFound', 'File not found: %s', filePath);
    end

    % Read entire input file as bytes
    fid = fopen(filePath, 'rb');
    if fid == -1, error('Cannot open file: %s', filePath); end
    cleaner = onCleanup(@() fclose(fid)); %#ok<NASGU>
    fseek(fid, 0, 'eof');
    nbytes = ftell(fid);
    fseek(fid, 0, 'bof');
    fileBytes = fread(fid, nbytes, 'uint8');

    % Validate leading magic
    if nbytes < 12 || ~isequal(fileBytes(1:4)', [80 65 82 49])
        error('Invalid Parquet: missing leading magic PAR1');
    end

    % Footer: [metadata][len(4LE)][PAR1]
    trailingMagic = fileBytes(end-3:end)';
    if ~isequal(trailingMagic, [80 65 82 49])
        error('Invalid Parquet: missing trailing magic PAR1');
    end

    footerLen = typecast(uint8(fileBytes(end-7:end-4)), 'uint32');
    footerLen = double(footerLen);
    if footerLen <= 0 || footerLen > nbytes - 12
        error('Invalid footer length: %d', footerLen);
    end

    metadataStart = nbytes - 8 - footerLen + 1; % MATLAB 1-based index
    metadataBytes = fileBytes(metadataStart : metadataStart + footerLen - 1);

    % Parse top-level fields as raw to preserve them
    parserRaw = parquet_metadata.ThriftCompactParser(metadataBytes);
    fields = parserRaw.readStructRaw();

    % Normalize user kvs into list of KeyValue entries (no key sanitization)
    kvListNew = normalizeToKeyValueList(kv);

    % Replace entire list with exactly the provided KVs
    newElems = encodeKeyValueElements(kvListNew);
    newHeader = encodeListHeader(numel(kvListNew), 12);
    field5Payload = [newHeader, newElems];

    % Reassemble FileMetaData struct: keep all fields except id 5, then insert new id 5
    kept = fields(arrayfun(@(f) f.id ~= 5, fields));
    % Append new field descriptor for id 5 with type LIST (9) and payload
    newField5.id = 5; newField5.type = 9; newField5.value = field5Payload;
    all = [kept, newField5];
    % Sort by id ascending
    [~, order] = sort([all.id]);
    all = all(order);

    % Serialize top-level struct
    top = uint8([]);
    lastId = 0;
    for i = 1:numel(all)
        f = all(i);
        delta = f.id - lastId;
        if delta >= 1 && delta <= 15
            header = uint8(bitshift(uint8(delta),4) + uint8(f.type));
            top = [top, header]; %#ok<AGROW>
        else
            header = uint8(f.type);
            top = [top, header, writeZigZagVarint(int64(f.id))]; %#ok<AGROW>
        end
        % Append value payload
        top = [top, uint8(f.value)]; %#ok<AGROW>
        lastId = f.id;
    end
    % STOP
    top = [top, uint8(0)];

    % Write output: in-place if outputFile equals inputFile, otherwise new file
    newLenLE = typecast(uint32(length(top)), 'uint8');
    % In-place: overwrite from metadataStart and truncate/grow as needed
    fidw = fopen(filePath, 'r+b');
    if fidw == -1, error('Cannot open file for update: %s', filePath); end
    cleaner2 = onCleanup(@() fclose(fidw)); %#ok<NASGU>
    fseek(fidw, metadataStart-1, 'bof');
    fwrite(fidw, top, 'uint8');
    fwrite(fidw, newLenLE, 'uint8');
    fwrite(fidw, uint8('PAR1'), 'uint8');
    % Truncate or extend to exact new end-of-file
    newEOF = (metadataStart-1) + length(top) + 8; % 4 len + 4 magic
    truncateFile(filePath, newEOF);
end

function list = normalizeToKeyValueList(kv)
    % Returns cell array of structs with fields: key, value
    list = {};
    if isa(kv, 'containers.Map')
        ks = kv.keys;
        list = cell(1, numel(ks));
        for i = 1:numel(ks)
            key = ks{i}; val = kv(key);
            list{i} = makeKV(key, val);
        end
    elseif isstruct(kv)
        fn = fieldnames(kv);
        list = cell(1, numel(fn));
        for i = 1:numel(fn)
            key = fn{i}; val = kv.(key);
            list{i} = makeKV(key, val);
        end
    elseif iscell(kv)
        % Expect Nx2 {key,value}
        list = cell(1, size(kv,1));
        for i = 1:size(kv,1)
            key = kv{i,1}; val = kv{i,2};
            list{i} = makeKV(key, val);
        end
    else
        error('Unsupported kv type. Use struct, containers.Map, or {key,value} cell.');
    end
end

function kv = makeKV(key, val)
    kv = struct();
    kv.key = ensureCharKey(key);
    if ischar(val) || (isstring(val) && isscalar(val))
        kv.value = char(val);
    else
        try
            kv.value = jsonencode(val);
        catch
            kv.value = char(string(val));
        end
    end
end

function header = encodeListHeader(n, elementType)
    n = double(n);
    if n < 15
        header = uint8(bitshift(uint8(n),4) + uint8(elementType));
    else
        header = [uint8(bitshift(uint8(15),4) + uint8(elementType)), writeUnsignedVarint(uint64(n))];
    end
end

function elems = encodeKeyValueElements(kvList)
    elems = uint8([]);
    for i = 1:numel(kvList)
        kv = kvList{i};
        % field_1: key (string)
        elems = [elems, uint8(bitshift(uint8(1),4) + uint8(8))]; %#ok<AGROW>
        elems = [elems, writeString(kv.key)]; %#ok<AGROW>
        % field_2: value (string)
        elems = [elems, uint8(bitshift(uint8(1),4) + uint8(8))]; %#ok<AGROW>
        elems = [elems, writeString(kv.value)]; %#ok<AGROW>
        % STOP
        elems = [elems, uint8(0)]; %#ok<AGROW>
    end
end

% (no list-parsing helpers needed in overwrite-only mode)

function b = writeString(str)
    if ~ischar(str)
        str = char(string(str));
    end
    % UTF-8 encode
    try
        utf8 = unicode2native(str, 'UTF-8');
    catch
        utf8 = uint8(str);
    end
    b = [writeUnsignedVarint(uint64(numel(utf8))), uint8(utf8)];
end

function b = writeUnsignedVarint(v)
    v = uint64(v);
    b = uint8([]);
    while true
        byte = bitand(v, uint64(127));
        v = bitshift(v, -7);
        if v ~= 0
            b = [b, uint8(bitor(byte, uint64(128)))]; %#ok<AGROW>
        else
            b = [b, uint8(byte)]; %#ok<AGROW>
            break;
        end
    end
end

function b = writeZigZagVarint(n)
    n = int64(n);
    zz = uint64(bitxor(bitshift(n, 1), bitshift(n, -63)));
    b = writeUnsignedVarint(zz);
end

function key = ensureCharKey(k)
    if isstring(k), key = char(k); elseif ischar(k), key = k; else, key = char(string(k)); end
end

% (no makeValidFieldName here; keys are written as provided)

function truncateFile(filePath, newSize)
    % Truncate or extend file to newSize bytes
    try
        fid = fopen(filePath, 'r+b');
        if fid == -1, error('Cannot open file to truncate: %s', filePath); end
        c = onCleanup(@() fclose(fid)); %#ok<NASGU>
        % Prefer MATLAB ftruncate if available
        try
            ftruncate(fid, newSize);
            return;
        catch
            % Fall back to Java RandomAccessFile
        end
    catch
        % If opening failed, try Java directly
    end
    raf = javaObject('java.io.RandomAccessFile', filePath, 'rw');
    raf.setLength(newSize);
    raf.close();
end
