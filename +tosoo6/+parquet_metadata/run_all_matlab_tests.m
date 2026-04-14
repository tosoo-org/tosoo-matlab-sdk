function run_all_matlab_tests
% Run all MATLAB read/write tests from repo root, path-robust
    scriptDir = fileparts(mfilename('fullpath'));
    if ~isempty(scriptDir), cd(scriptDir); end
    test_read_all;
    test_read_brotli_matlab;
    test_write_roundtrip_matlab;
    test_write_roundtrip_brotli_matlab;
    fprintf('All MATLAB tests passed.\n');
end

