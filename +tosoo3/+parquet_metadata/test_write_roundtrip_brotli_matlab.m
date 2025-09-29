function test_write_roundtrip_brotli_matlab
% Roundtrip test: inject metadata into a Brotli-compressed Parquet file

    src = fullfile('test_files','brotli_base.parquet');
    assert(isfile(src), 'Missing brotli base file. Run task gen first.');

    dst = fullfile('test_files','matlab_roundtrip_brotli.parquet');
    if isfile(dst), delete(dst); end
    copyfile(src, dst);

    kv = struct();
    kv.project = 'parquetmatlab';
    kv.author = 'matlab-writer-brotli';
    kv.version_num = 3;
    kv.config = struct('alpha', 0.3, 'beta', true, 'tags', {{'brotli','ok'}});

    parquet_metadata.writeMetadata(dst, kv);

    md = parquet_metadata.readMetadata(dst);

    assert(isfield(md, 'project') && strcmp(md.project, 'parquetmatlab'));
    assert(isfield(md, 'author') && strcmp(md.author, 'matlab-writer-brotli'));

    v = md.version_num;
    if ischar(v), assert(strcmp(v,'3')); else, assert(isequal(v,3)); end

    cfg = md.config; if ischar(cfg), cfg = jsondecode(cfg); end
    assert(isstruct(cfg) && abs(cfg.alpha - 0.3) < 1e-12);
    assert(isequal(cfg.beta, true));
    if isstring(cfg.tags), cfg.tags = cellstr(cfg.tags); end
    assert(iscell(cfg.tags) && numel(cfg.tags)>=2);

    fprintf('MATLAB roundtrip brotli test passed.\n');
end
