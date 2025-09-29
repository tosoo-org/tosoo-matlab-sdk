function test_write_roundtrip_matlab
% Test: write KV metadata in MATLAB and read it back in MATLAB
    % Ensure we run from the script directory
    scriptDir = fileparts(mfilename('fullpath'));
    if ~isempty(scriptDir), cd(scriptDir); end

    output = fullfile('test_files','matlab_roundtrip.parquet');
    if isfile(output), delete(output); end
    % Create a tiny Parquet file using MATLAB built-in writer
    T = table((1:5)', ["a";"b";"c";"d";"e"], 'VariableNames', {'id','label'});
    parquetwrite(output, T);

    % Define metadata to inject
    kv = struct();
    kv.project = 'parquetmatlab';
    kv.author = 'matlab-writer';
    kv.version_num = 1;
    kv.config = struct('alpha', 0.1, 'beta', true, 'tags', {{'a','b'}});

    parquet_metadata.writeMetadata(output, kv);

    md = parquet_metadata.readMetadata(output);

    assert(isfield(md, 'project') && strcmp(md.project, 'parquetmatlab'));
    assert(isfield(md, 'author') && strcmp(md.author, 'matlab-writer'));
    assert(isfield(md, 'version_num'));
    % version_num was JSON-encoded, so expect numeric or string after jsondecode
    if ischar(md.version_num)
        assert(strcmp(md.version_num, '1'));
    else
        assert(isequal(md.version_num, 1));
    end
    assert(isfield(md, 'config'));
    cfg = md.config;
    if ischar(cfg)
        cfg = jsondecode(cfg);
    end
    assert(isstruct(cfg) && isfield(cfg, 'alpha') && abs(cfg.alpha - 0.1) < 1e-12);
    assert(isfield(cfg, 'beta') && isequal(cfg.beta, true));

    fprintf('MATLAB roundtrip metadata test passed.\n');
end
