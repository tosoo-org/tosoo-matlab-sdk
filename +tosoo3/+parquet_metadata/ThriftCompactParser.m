classdef ThriftCompactParser < handle
    %THRIFTCOMPACTPARSER Minimal Thrift Compact Protocol parser for Parquet metadata
    %   This class implements the essential parts of Thrift Compact Protocol
    %   needed to parse Parquet FileMetaData structures.
    
    properties (Access = private)
        data            % uint8 array of bytes to parse
        pos             % Current position (1-based)
        last_field_id   % Last field ID seen (for delta encoding)
    end
    
    methods
        function obj = ThriftCompactParser(data)
            obj.data = data(:)';  % Ensure row vector
            obj.pos = 1;
            obj.last_field_id = 0;
        end
        
        function fields = readStructRaw(obj)
            % Read a Thrift struct and return raw fields (id, type, value bytes)
            % Does not build MATLAB values; only skips and captures payload bytes.
            fields = struct('id', {}, 'type', {}, 'value', {});
            obj.last_field_id = 0;
            
            while obj.pos <= length(obj.data)
                header_pos = obj.pos; %#ok<NASGU>
                field_header = obj.readByte();
                
                if field_header == 0
                    % STOP
                    break;
                end
                
                field_type = bitand(field_header, 15);
                delta = bitshift(field_header, -4);
                
                if delta == 0
                    field_id = obj.readZigZagVarint();
                else
                    field_id = obj.last_field_id + delta;
                end
                
                obj.last_field_id = field_id;
                
                value_start = obj.pos;
                % Consume value according to type, without keeping MATLAB value
                switch field_type
                    case {1, 2} % BOOLEAN_TRUE / BOOLEAN_FALSE: no payload
                        % nothing to consume
                    case 3  % BYTE
                        obj.readByte();
                    case {4,5,6} % I16 / I32 / I64
                        obj.readZigZagVarint();
                    case 7  % DOUBLE
                        obj.readDouble();
                    case 8  % BINARY/STRING
                        obj.readString();
                    case {9,10} % LIST / SET
                        obj.readList();
                    case 11 % MAP (not implemented in reader)
                        error('Map type not implemented');
                    case 12 % STRUCT
                        obj.readStruct();
                    otherwise
                        error('Unknown Thrift type: %d', field_type);
                end
                value_end = obj.pos;
                
                fields(end+1).id = double(field_id); %#ok<AGROW>
                fields(end).type = double(field_type);
                if value_end > value_start
                    fields(end).value = obj.data(value_start:value_end-1);
                else
                    fields(end).value = uint8([]);
                end
            end
        end
        
        function result = readStruct(obj)
            % Read a Thrift struct
            result = struct();
            obj.last_field_id = 0;
            field_count = 0; %#ok<NASGU>
            
            while obj.pos <= length(obj.data)
                field_count = field_count + 1;
                
                % Read field header
                field_header = obj.readByte();
                
                if field_header == 0
                    % STOP field
                    break;
                end
                
                % Parse field header
                field_type = bitand(field_header, 15);  % Lower 4 bits
                delta = bitshift(field_header, -4);     % Upper 4 bits
                
                if delta == 0
                    % Field ID follows
                    field_id = obj.readZigZagVarint();
                else
                    % Delta-encoded field ID
                    field_id = obj.last_field_id + delta;
                end
                
                % Validate field_id before using it
                if ~isfinite(field_id) || field_id < 0
                    error('Invalid field ID: %g at position %d', field_id, obj.pos);
                end
                
                obj.last_field_id = field_id;
                
                % Read field value based on type
                field_name = sprintf('field_%d', round(field_id));
                
                switch field_type
                    case 1  % BOOLEAN_TRUE
                        result.(field_name) = true;
                    case 2  % BOOLEAN_FALSE
                        result.(field_name) = false;
                    case 3  % BYTE
                        result.(field_name) = obj.readByte();
                    case 4  % I16
                        result.(field_name) = obj.readZigZagVarint();
                    case 5  % I32
                        result.(field_name) = obj.readZigZagVarint();
                    case 6  % I64
                        result.(field_name) = obj.readZigZagVarint();
                    case 7  % DOUBLE
                        result.(field_name) = obj.readDouble();
                    case 8  % BINARY/STRING
                        result.(field_name) = obj.readString();
                    case 9  % LIST
                        result.(field_name) = obj.readList();
                    case 10 % SET
                        result.(field_name) = obj.readList(); % Same as list
                    case 11 % MAP
                        result.(field_name) = obj.readMap();
                    case 12 % STRUCT
                        result.(field_name) = obj.readStruct();
                    otherwise
                        error('Unknown Thrift type: %d', field_type);
                end
            end
        end
        
        function value = readString(obj)
            % Read a string (length-prefixed binary)
            str_length = double(obj.readUnsignedVarint());
            
            if obj.pos + str_length - 1 > length(obj.data)
                error('String extends beyond data bounds');
            end
            
            bytes = obj.data(obj.pos : obj.pos + str_length - 1);
            obj.pos = obj.pos + str_length;
            
            % Convert bytes to string
            value = char(bytes);
        end
        
        function list = readList(obj)
            % Read a list
            size_and_type = obj.readByte();
            element_type = bitand(size_and_type, 15);
            size = bitshift(size_and_type, -4);
            
            if size == 15
                % Size follows as varint
                size = obj.readUnsignedVarint();
            end
            
            list = cell(1, size);
            
            % Save field ID context
            saved_field_id = obj.last_field_id;
            
            for i = 1:size
                obj.last_field_id = 0;  % Reset for each element
                
                switch element_type
                    case 1  % BOOLEAN_TRUE
                        list{i} = true;
                    case 2  % BOOLEAN_FALSE
                        list{i} = false;
                    case 3  % BYTE
                        list{i} = obj.readByte();
                    case 4  % I16
                        list{i} = obj.readZigZagVarint();
                    case 5  % I32
                        list{i} = obj.readZigZagVarint();
                    case 6  % I64
                        list{i} = obj.readZigZagVarint();
                    case 7  % DOUBLE
                        list{i} = obj.readDouble();
                    case 8  % BINARY/STRING
                        list{i} = obj.readString();
                    case 9  % LIST
                        list{i} = obj.readList();
                    case 10 % SET
                        list{i} = obj.readList();
                    case 12 % STRUCT
                        list{i} = obj.readStruct();
                    otherwise
                        error('Unsupported list element type: %d', element_type);
                end
            end
            
            % Restore field ID context
            obj.last_field_id = saved_field_id;
        end
        
        function map = readMap(obj)
            % Read a map (not implemented - not needed for metadata)
            error('Map type not implemented');
        end
        
        function value = readDouble(obj)
            % Read an 8-byte double (little endian)
            if obj.pos + 7 > length(obj.data)
                error('Double extends beyond data bounds');
            end
            
            bytes = obj.data(obj.pos : obj.pos + 7);
            obj.pos = obj.pos + 8;
            
            % Convert bytes to double (little endian)
            value = typecast(uint8(bytes), 'double');
        end
        
        function value = readByte(obj)
            % Read a single byte
            if obj.pos > length(obj.data)
                error('Byte read beyond data bounds');
            end
            
            value = obj.data(obj.pos);
            obj.pos = obj.pos + 1;
        end
        
        function value = readUnsignedVarint(obj)
            % Read an unsigned variable-length integer
            value = uint64(0);
            shift = 0;
            
            while true
                if obj.pos > length(obj.data)
                    error('Varint extends beyond data bounds at position %d', obj.pos);
                end
                
                byte = obj.data(obj.pos);
                obj.pos = obj.pos + 1;
                
                if shift < 64
                    value = value + bitshift(uint64(bitand(byte, 127)), shift);
                end
                
                if bitand(byte, 128) == 0
                    break;
                end
                
                shift = shift + 7;
                
                if shift >= 64
                    error('Varint too long (shift = %d)', shift);
                end
            end
        end
        
        function value = readZigZagVarint(obj)
            % Read a ZigZag-encoded signed varint
            unsigned_value = obj.readUnsignedVarint();
            
            % ZigZag decoding: (n >> 1) ^ (-(n & 1))
            % Convert to double first to avoid type mixing issues
            n = double(unsigned_value);
            value = bitxor(bitshift(n, -1), -bitand(n, 1));
        end
    end
end

