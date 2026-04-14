function [eeg_data, actigraphy_data] = read_data(input_file)
%READ_DATA Read data from parquet file and return EEG data and actigraphy data.
%   [EEG_DATA, ACTIGRAPHY_DATA] = READ_DATA(INPUT_FILE)
%   reads data from a Tosoo6 parquet file and returns two tables.
%
%   Input:
%       input_file - Path to the parquet file (string or char array)
%
%   Output:
%       eeg_data - Rows where is_eeg_sample=true (table)
%       actigraphy_data - Rows where is_actigraphy_sample=true (table)
%
%   Note: The views are separate tables in MATLAB (not references like in Python)
%   If the columns "is_eeg_sample" and "is_actigraphy_sample" do not exist,
%   all data is assumed to be EEG data (returned in eeg_data).
%
%   Example:
%       [eeg, acti] = tosoo6.read_data('recording.tosoo6.parquet');

    % Validate input
    if ~isfile(input_file)
        error('tosoo6:read_data:FileNotFound', 'File not found: %s', input_file);
    end

    % Read the parquet file using MATLAB's built-in parquetread
    full_data = parquetread(input_file);

    % Check if sample type columns exist
    has_eeg_col = ismember('is_eeg_sample', full_data.Properties.VariableNames);
    has_acti_col = ismember('is_actigraphy_sample', full_data.Properties.VariableNames);

    % If neither column exists, assume all data is EEG
    if ~has_eeg_col && ~has_acti_col
        eeg_data = full_data;
        % Return empty actigraphy table with only relevant columns
        acti_cols = {'ms_since_first_sample', 'acceleration_x', 'acceleration_y', 'acceleration_z'};
        acti_cols = acti_cols(ismember(acti_cols, full_data.Properties.VariableNames));
        actigraphy_data = full_data(false(height(full_data), 1), acti_cols);
        return;
    end

    % Create EEG data view - filter rows where is_eeg_sample is true
    if has_eeg_col
        eeg_data = full_data(full_data.is_eeg_sample, :);
    else
        % If column doesn't exist, return empty table with same structure
        eeg_data = full_data(false(height(full_data), 1), :);
        warning('tosoo6:read_data:NoEEGColumn', 'Column "is_eeg_sample" not found in data');
    end

    % Remove actigraphy-specific columns from EEG view
    eeg_cols_to_remove = {'is_eeg_sample', 'is_actigraphy_sample', ...
                          'acceleration_x', 'acceleration_y', 'acceleration_z'};
    eeg_cols_to_remove = eeg_cols_to_remove(ismember(eeg_cols_to_remove, eeg_data.Properties.VariableNames));
    if ~isempty(eeg_cols_to_remove)
        eeg_data(:, eeg_cols_to_remove) = [];
    end

    % Create actigraphy data view - filter rows where is_actigraphy_sample is true
    if has_acti_col
        actigraphy_data = full_data(full_data.is_actigraphy_sample, :);
    else
        % If column doesn't exist, return empty table with same structure
        actigraphy_data = full_data(false(height(full_data), 1), :);
        warning('tosoo6:read_data:NoActigraphyColumn', 'Column "is_actigraphy_sample" not found in data');
    end

    % Keep only relevant columns for actigraphy view
    acti_cols_to_keep = {'ms_since_first_sample', 'acceleration_x', 'acceleration_y', 'acceleration_z'};
    acti_cols_to_keep = acti_cols_to_keep(ismember(acti_cols_to_keep, actigraphy_data.Properties.VariableNames));
    actigraphy_data = actigraphy_data(:, acti_cols_to_keep);
end