function test_read_brotli_matlab
% Read test: validate metadata from Brotli-compressed base Parquet file

    path = fullfile('test_files','brotli_base.parquet');
    assert(isfile(path), 'Missing brotli base file. Run task gen first.');

    md = parquet_metadata.readMetadata(path);
    assert(isfield(md, 'title') && strcmp(md.title, 'Brotli Base Test'));
    assert(isfield(md, 'author') && strcmp(md.author, 'pyarrow'));
    assert(isfield(md, 'project') && strcmp(md.project, 'parquetmatlab'));
    assert(isfield(md, 'compression') && strcmp(md.compression, 'brotli'));
    assert(isfield(md, 'version') && strcmp(md.version, '1'));

    fprintf('MATLAB brotli read test passed.\n');
end
